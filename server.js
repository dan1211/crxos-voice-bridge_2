/**
 * CRX.OS ISA — Live Voice Bridge Server
 * Bridges Twilio Media Streams ↔ OpenAI Realtime API
 */

import Fastify from "fastify";
import FastifyWS from "@fastify/websocket";
import { WebSocket } from "ws";

const fastify = Fastify({ logger: true });
fastify.register(FastifyWS);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-realtime-2";
const OPENAI_REALTIME_VOICE =
  process.env.OPENAI_REALTIME_VOICE || "coral";

const BASE44_URL = process.env.BASE44_FUNCTION_URL;
const BASE44_API_KEY = process.env.BASE44_API_KEY;
const BASE44_BRIDGE_URL = process.env.BASE44_BRIDGE_URL;
const BRIDGE_SHARED_SECRET = process.env.BRIDGE_SHARED_SECRET;

const AGENT_TRANSFER_NUMBER = process.env.AGENT_TRANSFER_NUMBER;
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_API_KEY_SID = process.env.TWILIO_API_KEY_SID;
const TWILIO_API_KEY_SECRET = process.env.TWILIO_API_KEY_SECRET;

const WS_OPEN = WebSocket.OPEN;

function safeJsonParse(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function safeSend(ws, payload) {
  if (ws && ws.readyState === WS_OPEN) {
    ws.send(typeof payload === "string" ? payload : JSON.stringify(payload));
    return true;
  }
  return false;
}

function buildTwilioAuthHeader() {
  return (
    "Basic " +
    Buffer.from(`${TWILIO_API_KEY_SID}:${TWILIO_API_KEY_SECRET}`).toString(
      "base64"
    )
  );
}

function escapeXml(value = "") {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function normalizePhone(value = "") {
  const digits = String(value).replace(/\D/g, "");

  if (digits.length === 10) return `1${digits}`;
  if (digits.length === 11 && digits.startsWith("1")) return digits;

  return digits;
}

async function callBase44(action, body) {
  if (action === "get_lead_context") {
    const res = await fetch(BASE44_BRIDGE_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-bridge-secret": BRIDGE_SHARED_SECRET,
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Bridge function failed (${res.status}): ${text}`);
    }

    return res.json();
  }

  const res = await fetch(`${BASE44_URL}?action=${encodeURIComponent(action)}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": BASE44_API_KEY,
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Base44 ${action} failed (${res.status}): ${text}`);
  }

  return res.json();
}

fastify.get("/", async () => ({
  status: "CRX.OS Voice Bridge running",
  openai_model: OPENAI_REALTIME_MODEL,
  openai_voice: OPENAI_REALTIME_VOICE,
}));

fastify.register(async (fastify) => {
  fastify.get("/media-stream", { websocket: true }, async (connection, request) => {
    const twilioWs = connection.socket;

    let leadId = null;
    let callSid = null;
    let streamSid = "";

    let lead = null;
    let realtorProfile = null;
    let calendlyLink = "";
    let systemPrompt = "";

    let openAiWs = null;
    let sessionReady = false;
    let greetingStarted = false;
    let sessionTimer = null;
    let responseActive = false;

    fastify.log.info("Twilio WebSocket opened — waiting for start event");

    function closeBoth() {
      if (sessionTimer) {
        clearTimeout(sessionTimer);
        sessionTimer = null;
      }

      if (openAiWs?.readyState === WS_OPEN) openAiWs.close();
      if (twilioWs.readyState === WS_OPEN) twilioWs.close();
    }

    async function openOpenAIRealtimeSession() {
      fastify.log.info(
        `Opening OpenAI Realtime WS with model: ${OPENAI_REALTIME_MODEL}`
      );

      openAiWs = new WebSocket(
        `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(
          OPENAI_REALTIME_MODEL
        )}`,
        {
          headers: {
            Authorization: `Bearer ${OPENAI_API_KEY}`,
          },
        }
      );

      sessionTimer = setTimeout(() => {
        if (!sessionReady) {
          fastify.log.error(
            "OpenAI session did not become ready within 8 seconds"
          );
          closeBoth();
        }
      }, 8000);

      openAiWs.on("open", () => {
        fastify.log.info("OpenAI Realtime WS opened");

        safeSend(openAiWs, {
          type: "session.update",
          session: {
            type: "realtime",
            model: OPENAI_REALTIME_MODEL,
            instructions: systemPrompt,

            output_modalities: ["audio"],

            reasoning: {
              effort: "low",
            },

            audio: {
              input: {
                format: {
                  type: "audio/pcmu",
                },
                turn_detection: {
                  type: "server_vad",
                  threshold: 0.45,
                  prefix_padding_ms: 300,
                  silence_duration_ms: 600,
                },
              },
              output: {
                format: {
                  type: "audio/pcmu",
                },
                voice: OPENAI_REALTIME_VOICE,
              },
            },

            tools: getTools(),
            tool_choice: "auto",
          },
        });
      });

      openAiWs.on("message", async (aiRaw) => {
        const aiMsg = safeJsonParse(aiRaw);
        if (!aiMsg) return;

        fastify.log.info(`OpenAI event: ${aiMsg.type}`);

        if (aiMsg.type === "response.created") {
          responseActive = true;
        }

        if (aiMsg.type === "response.done") {
          responseActive = false;
        }

        if (aiMsg.type === "input_audio_buffer.speech_started") {
          fastify.log.info("User started speaking");

          if (responseActive && openAiWs?.readyState === WS_OPEN) {
            fastify.log.info("Cancelling active OpenAI response");
            safeSend(openAiWs, {
              type: "response.cancel",
            });
            responseActive = false;
          } else {
            fastify.log.info("No active OpenAI response to cancel");
          }

          return;
        }

        if (aiMsg.type === "error") {
          fastify.log.error(`OpenAI error: ${JSON.stringify(aiMsg.error)}`);
          return;
        }

        if (aiMsg.type === "session.created") {
          fastify.log.info("OpenAI session created");
          return;
        }

        if (aiMsg.type === "session.updated") {
          fastify.log.info("OpenAI session ready");

          if (sessionTimer) {
            clearTimeout(sessionTimer);
            sessionTimer = null;
          }

          sessionReady = true;

          if (!greetingStarted) {
            greetingStarted = true;

            safeSend(openAiWs, {
              type: "conversation.item.create",
              item: {
                type: "message",
                role: "user",
                content: [
                  {
                    type: "input_text",
                    text:
                      "Start the phone call now. Greet the lead naturally as Emma, the virtual assistant for The Fugate Team. Confirm you are speaking with the lead and ask if now is an okay time. After that question, stop speaking and wait.",
                  },
                ],
              },
            });

            safeSend(openAiWs, {
              type: "response.create",
              response: {
                output_modalities: ["audio"],
              },
            });
          }

          return;
        }

        if (
          (aiMsg.type === "response.audio.delta" ||
            aiMsg.type === "response.output_audio.delta") &&
          aiMsg.delta &&
          streamSid &&
          twilioWs.readyState === WS_OPEN
        ) {
          safeSend(twilioWs, {
            event: "media",
            streamSid,
            media: {
              payload: aiMsg.delta,
            },
          });

          return;
        }

        if (aiMsg.type === "response.function_call_arguments.done") {
          fastify.log.info(`OpenAI tool call: ${aiMsg.name}`);

          const args = safeJsonParse(aiMsg.arguments || "{}") || {};

          await handleToolCall(aiMsg.name, args, aiMsg.call_id, {
            lead,
            realtorProfile,
            callSid,
            calendlyLink,
            openAiWs,
            twilioWs,
            leadId,
          });

          return;
        }
      });

      openAiWs.on("close", (code, reason) => {
        if (sessionTimer) {
          clearTimeout(sessionTimer);
          sessionTimer = null;
        }

        fastify.log.info(
          `OpenAI WS closed — code: ${code}, reason: ${reason?.toString()}`
        );

        if (leadId || callSid) {
          callBase44("voice_call_ended", {
            lead_id: leadId,
            call_sid: callSid,
            close_code: code,
            close_reason: reason?.toString() || "",
          }).catch((err) => {
            fastify.log.warn(`voice_call_ended log failed: ${err.message}`);
          });
        }

        if (twilioWs.readyState === WS_OPEN) twilioWs.close();
      });

      openAiWs.on("error", (err) => {
        fastify.log.error(`OpenAI WS error: ${err.message}`);
      });
    }

    twilioWs.on("message", async (raw) => {
      const msg = safeJsonParse(raw);
      if (!msg) return;

      if (msg.event === "connected") {
        fastify.log.info("Twilio connected event received");
        return;
      }

      if (msg.event === "start") {
        streamSid = msg.start?.streamSid || "";
        callSid = msg.start?.callSid || "";

        leadId =
          msg.start?.customParameters?.lead_id ||
          request.query?.lead_id ||
          null;

        fastify.log.info(
          `Twilio start — streamSid: ${streamSid}, callSid: ${callSid}, lead_id: ${leadId}`
        );

        fastify.log.info(
          `Twilio customParameters: ${JSON.stringify(
            msg.start?.customParameters || {}
          )}`
        );

        if (!leadId) {
          fastify.log.error("No lead_id found. Closing Twilio connection.");
          closeBoth();
          return;
        }

        try {
          const data = await callBase44("get_lead_context", {
            lead_id: leadId,
          });

          lead = data.lead;
          realtorProfile = data.realtorProfile;
          calendlyLink = realtorProfile?.calendly_link || "";
          systemPrompt = buildSystemPrompt(lead, realtorProfile);

          fastify.log.info(
            `Lead loaded: ${lead?.name || "Unknown"} (${leadId})`
          );
        } catch (err) {
          fastify.log.error(`Failed to load lead context: ${err.message}`);
          closeBoth();
          return;
        }

        await openOpenAIRealtimeSession();
        return;
      }

      if (msg.event === "media") {
        if (!openAiWs) return;

        const audioPayload = msg.media?.payload;
        if (!audioPayload) return;

        if (sessionReady && openAiWs.readyState === WS_OPEN) {
          safeSend(openAiWs, {
            type: "input_audio_buffer.append",
            audio: audioPayload,
          });
        }

        return;
      }

      if (msg.event === "stop") {
        fastify.log.info(`Twilio stream stopped for lead: ${leadId}`);

        if (openAiWs?.readyState === WS_OPEN) {
          openAiWs.close();
        }

        return;
      }
    });

    twilioWs.on("close", () => {
      fastify.log.info(`Twilio WS closed for lead: ${leadId}`);

      if (openAiWs?.readyState === WS_OPEN) {
        openAiWs.close();
      }
    });

    twilioWs.on("error", (err) => {
      fastify.log.error(`Twilio WS error: ${err.message}`);
    });
  });
});

function getTools() {
  return [
    {
      type: "function",
      name: "qualify_lead",
      description:
        "Save qualification details collected during the call. Use when you have learned at least one meaningful qualification detail.",
      parameters: {
        type: "object",
        properties: {
          lead_type: { type: "string", enum: ["buyer", "seller"] },
          timeline: { type: "string" },
          budget: { type: "string" },
          pre_approved: { type: "boolean" },
          working_with_agent: { type: "boolean" },
          notes: { type: "string" },
        },
        required: ["lead_type"],
      },
    },
    {
      type: "function",
      name: "transfer_to_agent",
      description:
        "Transfer the active call to Daniel. Use when the lead asks for a human, Daniel, Sarah, an agent, a phone call, showing help, offer help, or immediate help.",
      parameters: {
        type: "object",
        properties: {
          reason: { type: "string" },
          summary: { type: "string" },
        },
        required: ["reason"],
      },
    },
    {
      type: "function",
      name: "book_appointment",
      description:
        "Send the lead a Calendly scheduling link by SMS. Use when the lead wants to schedule, asks for a link, or prefers not to transfer live.",
      parameters: {
        type: "object",
        properties: {
          preferred_time: { type: "string" },
        },
      },
    },
    {
      type: "function",
      name: "end_call",
      description:
        "End the call only when the conversation is complete, the lead clearly wants to end, it is a wrong number, or the lead asks not to be contacted.",
      parameters: {
        type: "object",
        properties: {
          reason: {
            type: "string",
            enum: [
              "completed",
              "not_interested",
              "dnc_requested",
              "wrong_number",
              "busy",
              "no_answer",
            ],
          },
          summary: { type: "string" },
        },
        required: ["reason"],
      },
    },
    {
      type: "function",
      name: "wait_for_user",
      description:
        "Use when the latest audio is silence, background noise, hold music, TV audio, side conversation, or speech not addressed to the assistant. This should not produce a spoken reply.",
      parameters: {
        type: "object",
        properties: {},
        required: [],
      },
    },
  ];
}

async function handleToolCall(
  name,
  args,
  callId,
  { lead, realtorProfile, callSid, calendlyLink, openAiWs, twilioWs, leadId }
) {
  const ack = (output, createAudioResponse = true) => {
    safeSend(openAiWs, {
      type: "conversation.item.create",
      item: {
        type: "function_call_output",
        call_id: callId,
        output: JSON.stringify(output),
      },
    });

    if (createAudioResponse) {
      safeSend(openAiWs, {
        type: "response.create",
        response: {
          output_modalities: ["audio"],
        },
      });
    }
  };

  if (name === "wait_for_user") {
    ack({ success: true, action: "waiting" }, false);
    return;
  }

  if (name === "qualify_lead") {
    await callBase44("qualify_lead_from_call", {
      lead_id: leadId,
      ...args,
    }).catch((err) => {
      fastify.log.warn(`qualify_lead_from_call failed: ${err.message}`);
    });

    ack({ success: true, response_text: "Got it." });
    return;
  }

  if (name === "book_appointment") {
    try {
      const result = await callBase44("send_calendly_sms", {
        lead_id: leadId,
        preferred_time: args.preferred_time || "",
        calendly_link: calendlyLink,
      });

      if (!result?.success) {
        throw new Error(result?.error || "Calendly SMS failed");
      }

      ack({
        success: true,
        response_text:
          "I just texted you the scheduling link. You can use that to pick a time that works best.",
      });
    } catch (err) {
      fastify.log.error(`send_calendly_sms failed: ${err.message}`);

      ack({
        success: false,
        response_text:
          "I could not send the scheduling link by text right now, but I will make sure the team follows up with you.",
        error: err.message,
      });
    }

    return;
  }

  if (name === "transfer_to_agent") {
    try {
      const leadPhone = normalizePhone(lead?.phone);
      const transferPhone = normalizePhone(AGENT_TRANSFER_NUMBER);

      if (leadPhone && transferPhone && leadPhone === transferPhone) {
        fastify.log.warn(
          `Transfer blocked because lead phone and transfer number are the same: ${transferPhone}`
        );

        ack({
          success: false,
          response_text:
            "I cannot connect the call to the same number you are calling from, but I will make sure the team follows up with you.",
        });

        return;
      }

      await callBase44("log_transfer", {
        lead_id: leadId,
        reason: args.reason || "",
        summary: args.summary || "",
      }).catch((err) => {
        fastify.log.warn(`log_transfer failed: ${err.message}`);
      });

      const callerId = realtorProfile?.twilio_phone_number || "";

      const transferTwiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Dial timeout="25" answerOnBridge="true"${
    callerId ? ` callerId="${escapeXml(callerId)}"` : ""
  }>
    <Number>${escapeXml(AGENT_TRANSFER_NUMBER)}</Number>
  </Dial>
  <Hangup/>
</Response>`;

      fastify.log.info(`Transfer TwiML: ${transferTwiml}`);
      fastify.log.info(`Attempting transfer for callSid: ${callSid}`);
      fastify.log.info(`Transfer destination: ${AGENT_TRANSFER_NUMBER}`);

      const res = await fetch(
        `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${callSid}.json`,
        {
          method: "POST",
          headers: {
            Authorization: buildTwilioAuthHeader(),
            "Content-Type": "application/x-www-form-urlencoded",
          },
          body: new URLSearchParams({
            Twiml: transferTwiml,
          }),
        }
      );

      const text = await res.text();

      if (!res.ok) {
        fastify.log.error(`Twilio transfer failed (${res.status}): ${text}`);
        throw new Error(`Twilio transfer failed (${res.status}): ${text}`);
      }

      fastify.log.info(`Twilio transfer accepted: ${text}`);

      ack({ success: true, response_text: "Transfer initiated." }, false);
    } catch (err) {
      fastify.log.error(`Transfer failed: ${err.message}`);

      ack({
        success: false,
        response_text:
          "I could not connect the call right now, but I will make sure the team follows up with you.",
        error: err.message,
      });
    }

    return;
  }

  if (name === "end_call") {
    await callBase44("close_call", {
      lead_id: leadId,
      reason: args.reason,
      summary: args.summary || "",
    }).catch((err) => {
      fastify.log.warn(`close_call failed: ${err.message}`);
    });

    ack({
      success: true,
      response_text: "Thanks for your time. Have a great day.",
    });

    setTimeout(() => {
      if (twilioWs.readyState === WS_OPEN) {
        twilioWs.close();
      }
    }, 4500);

    return;
  }

  ack({
    success: false,
    response_text:
      "I could not complete that action, but I can still help with the call.",
    error: `Unknown tool: ${name}`,
  });
}

function buildSystemPrompt(lead, realtorProfile) {
  const firstName = lead?.name?.split(" ")[0] || "there";
  const source = lead?.source || "your inquiry";
  const leadType = lead?.lead_type || "buyer";

  const property = lead?.property_address
    ? `They originally inquired about ${lead.property_address}${
        lead.property_price ? ` listed at ${lead.property_price}` : ""
      }.`
    : "No specific property was attached to this lead.";

  const inquiryMessage = lead?.inquiry_message
    ? `Original inquiry message: "${lead.inquiry_message}".`
    : "No original inquiry message was attached.";

  return `
# Role and Objective

You are Emma, the virtual assistant for The Fugate Team.
You are helping Daniel and Sarah Fugate follow up with real estate leads.
You are on a live outbound phone call with ${firstName}, who came in from ${source}.
Lead type: ${leadType}.
${property}
${inquiryMessage}

Your objective is to quickly determine how The Fugate Team can help, qualify the lead, and either connect them with Daniel or send a scheduling link.

# Identity Disclosure

When introducing yourself, say you are Emma, the virtual assistant for The Fugate Team.

Do not pretend to be a human agent.
Do not say you are a real estate agent.
Do not say you are Daniel or Sarah.

If asked whether you are AI, say:
"Yes, I’m Emma, the virtual assistant for The Fugate Team. I help follow up quickly and get you connected with the right person."

# Personality and Tone

- Warm, friendly, upbeat, and professional.
- Sound like a helpful virtual assistant for a real estate team.
- Speak at a normal conversational phone pace.
- Sound confident and energetic.
- Be conversational rather than transactional.
- Use contractions naturally.
- Use short spoken sentences.
- Avoid sounding scripted.
- Avoid sounding like a call center.
- Avoid sounding like a questionnaire.
- Do not over-explain.
- Do not narrate your reasoning.
- Do not say what you are thinking.
- Do not use filler phrases like "let me think."
- Vary your wording naturally.

# Conversation Style

Before asking a follow-up question, briefly acknowledge what the caller just said.

Examples:

- "Gotcha."
- "That makes sense."
- "Okay."
- "Sure."
- "I understand."
- "Absolutely."

Keep acknowledgements short and natural.

Do not acknowledge every single sentence.

The conversation should feel like a real discussion, not a questionnaire.

# Turn Taking

- After asking a question, stop speaking and wait for the caller.
- Do not continue talking if the caller starts to speak.
- If the caller interrupts, stop and listen.
- Do not talk over the caller.
- A short natural pause is fine, but do not create long awkward pauses.

# Preambles

Do not say:
- "Let me think"
- "Let me think about that"
- "Let me think about the best next step"
- "Hmm"
- "One moment while I process that"
- "I am thinking"
- "I need to think"
- "best next step"

Most of the time, do not use a preamble at all.

Only use a short action phrase when you are about to call a tool or transfer the caller.

Allowed action phrases:
- "I can help with that."
- "I can connect you now."
- "I can text that to you."
- "I’ll note that for the team."
- "Absolutely. I can try to connect you with Daniel now."
- "No problem. I can text you the scheduling link."

After the action phrase, immediately continue with the next useful action.

# Conversation Flow

## 1. Greeting

Start naturally:
"Hi, is this ${firstName}? This is Emma with The Fugate Team. I saw that you had reached out about real estate and wanted to follow up. Do you have a quick minute?"

After asking that question, stop speaking and wait.

If they are busy, offer to text a scheduling link or ask when a better time would be.
Only use end_call if the caller clearly wants to end the conversation.

## 2. Discovery

Your goal is to naturally learn:

- whether they are buying or selling
- what they are looking for
- their timeline
- financing status if relevant
- whether they are already working with an agent

Do not ask these questions in a fixed order.

Follow the caller's lead and ask the most relevant next question based on what they just said.

Have a natural conversation instead of conducting an interview.

If the caller volunteers information, do not ask a question that has already been answered.

Only ask one question at a time.
## 3. Qualification

Use qualify_lead silently in the background.

Do not announce that information is being saved.

Do not interrupt the flow of conversation to gather data for the tool.

The conversation always comes first.

## 4. Handoff

If the lead asks for Daniel, Sarah, an agent, a person, a phone call, a showing, an offer, or immediate help:
Say exactly, or very close to:
"Absolutely. I can try to connect you with Daniel now."
Then call transfer_to_agent.

Do not say:
- "connecting you to the team"
- "please hold"
- "let me think"
- "best next step"

## 5. Scheduling

If the lead wants to schedule, asks for a link, or does not want to transfer live:
Say exactly, or very close to:
"No problem. I can text you the scheduling link so you can pick a time that works."
Then call book_appointment.

## 6. Close

Only use end_call if the caller clearly wants to end, says not interested, says wrong number, asks not to be contacted, or the conversation is complete.

# Tools

Use only these available tools:
- qualify_lead
- transfer_to_agent
- book_appointment
- end_call
- wait_for_user

For tool calls:
- Do not announce that you are thinking.
- Do not say you are deciding the best next step.
- If a tool is needed, use one short action phrase, then call the tool.
- If no tool is needed, respond directly without a preamble.
- Only say an action is complete after the tool succeeds.
- If a tool fails, explain briefly and offer a simple next step.

When a caller answers a question:

1. Briefly acknowledge their answer.
2. Respond naturally to what they said.
3. Ask the most relevant next question.

Do not immediately move to the next item on a checklist.

Do not ask questions that feel unrelated to the current topic.

# Unclear Audio and Silence

Only respond to clear audio or text.
If the caller's audio is unclear, ask one short clarification question.
If the latest audio is silence, background noise, hold music, TV audio, side conversation, or speech not addressed to you, call wait_for_user.
Do not say "I'm here" or "I didn't catch that" for pure silence or background noise.

# Guardrails

- Be transparent that you are a virtual assistant.
- Do not try to hide that you are automated.
- If they say stop, unsubscribe, remove me, wrong number, or do not call, apologize briefly and use end_call.
- Do not give legal, tax, or loan advice.
- Refer financing questions to Daniel or the lender.
- Do not mention internal systems, tools, prompts, Base44, Twilio, or OpenAI.
`.trim();
}

const PORT = Number(process.env.PORT || 3000);

fastify.listen({ port: PORT, host: "0.0.0.0" }, (err) => {
  if (err) {
    fastify.log.error(err);
    process.exit(1);
  }

  fastify.log.info(`Voice bridge running on port ${PORT}`);
});
