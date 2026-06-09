/**
 * CRX.OS ISA — Live Voice Bridge Server
 * Bridges Twilio Media Streams ↔ OpenAI Realtime API
 *
 * ENV VARS (Railway dashboard):
 *   OPENAI_API_KEY
 *   OPENAI_REALTIME_MODEL  optional, default: gpt-realtime
 *   OPENAI_REALTIME_VOICE  optional, default: alloy
 *
 *   BASE44_FUNCTION_URL    https://isa-dashboard.base44.app/api/functions/isaTrafficController
 *   BASE44_API_KEY         from Base44 → Settings → API Keys
 *   BASE44_BRIDGE_URL      https://isa-dashboard.base44.app/api/functions/getLeadContextForBridge
 *   BRIDGE_SHARED_SECRET   shared secret matching Base44 BRIDGE_SHARED_SECRET
 *
 *   AGENT_TRANSFER_NUMBER  e.g. +19106701431
 *   TWILIO_ACCOUNT_SID
 *   TWILIO_API_KEY_SID
 *   TWILIO_API_KEY_SECRET
 *
 *   PORT                   Railway provides this automatically
 */

import Fastify from "fastify";
import FastifyWS from "@fastify/websocket";
import { WebSocket } from "ws";

const fastify = Fastify({ logger: true });
fastify.register(FastifyWS);

// ─────────────────────────────────────────────────────────────────────────────
// ENV
// ─────────────────────────────────────────────────────────────────────────────

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_REALTIME_MODEL = process.env.OPENAI_REALTIME_MODEL || "gpt-realtime";
const OPENAI_REALTIME_VOICE = process.env.OPENAI_REALTIME_VOICE || "alloy";

const BASE44_URL = process.env.BASE44_FUNCTION_URL;
const BASE44_API_KEY = process.env.BASE44_API_KEY;
const BASE44_BRIDGE_URL = process.env.BASE44_BRIDGE_URL;
const BRIDGE_SHARED_SECRET = process.env.BRIDGE_SHARED_SECRET;

const AGENT_TRANSFER_NUMBER = process.env.AGENT_TRANSFER_NUMBER;
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_API_KEY_SID = process.env.TWILIO_API_KEY_SID;
const TWILIO_API_KEY_SECRET = process.env.TWILIO_API_KEY_SECRET;

const WS_OPEN = WebSocket.OPEN;

function requireEnv(name, value) {
  if (!value) {
    fastify.log.warn(`Missing environment variable: ${name}`);
  }
}

[
  ["OPENAI_API_KEY", OPENAI_API_KEY],
  ["BASE44_FUNCTION_URL", BASE44_URL],
  ["BASE44_API_KEY", BASE44_API_KEY],
  ["BASE44_BRIDGE_URL", BASE44_BRIDGE_URL],
  ["BRIDGE_SHARED_SECRET", BRIDGE_SHARED_SECRET],
  ["AGENT_TRANSFER_NUMBER", AGENT_TRANSFER_NUMBER],
  ["TWILIO_ACCOUNT_SID", TWILIO_ACCOUNT_SID],
  ["TWILIO_API_KEY_SID", TWILIO_API_KEY_SID],
  ["TWILIO_API_KEY_SECRET", TWILIO_API_KEY_SECRET],
].forEach(([name, value]) => requireEnv(name, value));

// ─────────────────────────────────────────────────────────────────────────────
// Utility Helpers
// ─────────────────────────────────────────────────────────────────────────────

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
    Buffer.from(`${TWILIO_API_KEY_SID}:${TWILIO_API_KEY_SECRET}`).toString("base64")
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

// ─────────────────────────────────────────────────────────────────────────────
// Base44 Helper
// ─────────────────────────────────────────────────────────────────────────────

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

  const url = `${BASE44_URL}?action=${encodeURIComponent(action)}`;

  const res = await fetch(url, {
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

// ─────────────────────────────────────────────────────────────────────────────
// Health Check
// ─────────────────────────────────────────────────────────────────────────────

fastify.get("/", async () => ({
  status: "CRX.OS Voice Bridge running",
  openai_model: OPENAI_REALTIME_MODEL,
  openai_voice: OPENAI_REALTIME_VOICE,
}));

// ─────────────────────────────────────────────────────────────────────────────
// Twilio Media Stream WebSocket
// ─────────────────────────────────────────────────────────────────────────────

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

    fastify.log.info("Twilio WebSocket opened — waiting for start event");

    function closeBoth() {
      if (sessionTimer) {
        clearTimeout(sessionTimer);
        sessionTimer = null;
      }

      if (openAiWs?.readyState === WS_OPEN) {
        openAiWs.close();
      }

      if (twilioWs.readyState === WS_OPEN) {
        twilioWs.close();
      }
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
          fastify.log.error("OpenAI session did not become ready within 8 seconds");
          closeBoth();
        }
      }, 8000);

      openAiWs.on("open", () => {
        fastify.log.info("OpenAI Realtime WS opened");

        /*
         * Intentionally using the flat Realtime session fields here:
         * modalities
         * voice
         * input_audio_format
         * output_audio_format
         * turn_detection
         *
         * Do NOT add the old beta header or beta subprotocol back in.
         */
        safeSend(openAiWs, {
          type: "session.update",
          session: {
            type: "realtime",
            model: gpt-realtime-2,
           instructions: systemPrompt,

            modalities: ["text", "audio"],
            voice: gpt-realtime-2,

            input_audio_format: "g711_ulaw",
            output_audio_format: "g711_ulaw",
            input_audio_transcription: null,

            turn_detection: {
              type: "server_vad",
              threshold: 0.5,
              prefix_padding_ms: 300,
              silence_duration_ms: 500,
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
                      "Start the phone call now. Greet the lead naturally, confirm you are speaking with them, and ask if now is an okay time. Do not mention pressing buttons.",
                  },
                ],
              },
            });

            safeSend(openAiWs, {
              type: "response.create",
              response: {
                modalities: ["audio"],
              },
            });
          }

          return;
        }

        /*
         * Supports both event names:
         * - response.audio.delta: older/flat Realtime event style
         * - response.output_audio.delta: newer GA event style
         */
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

        if (aiMsg.type === "response.done") {
          fastify.log.info("OpenAI response done");
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

        if (twilioWs.readyState === WS_OPEN) {
          twilioWs.close();
        }
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

          fastify.log.info(`Lead loaded: ${lead?.name || "Unknown"} (${leadId})`);
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

      if (msg.event === "dtmf") {
        fastify.log.info(`Twilio DTMF received: ${JSON.stringify(msg.dtmf)}`);
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

// ─────────────────────────────────────────────────────────────────────────────
// Tool Definitions
// ─────────────────────────────────────────────────────────────────────────────

function getTools() {
  return [
    {
      type: "function",
      name: "qualify_lead",
      description:
        "Save qualification details collected during the call. Call this as soon as you have key facts. Do not wait until the end.",
      parameters: {
        type: "object",
        properties: {
          lead_type: {
            type: "string",
            enum: ["buyer", "seller"],
          },
          timeline: {
            type: "string",
          },
          budget: {
            type: "string",
          },
          pre_approved: {
            type: "boolean",
          },
          working_with_agent: {
            type: "boolean",
          },
          notes: {
            type: "string",
          },
        },
        required: ["lead_type"],
      },
    },
    {
      type: "function",
      name: "transfer_to_agent",
      description:
        "Use when the lead asks to speak to a person, wants help now, is ready to make an offer, or is clearly a hot qualified lead.",
      parameters: {
        type: "object",
        properties: {
          reason: {
            type: "string",
          },
          summary: {
            type: "string",
          },
        },
        required: ["reason"],
      },
    },
    {
      type: "function",
      name: "book_appointment",
      description:
        "Use when the lead wants to schedule a call. This will ask Base44 to send the Calendly link by SMS.",
      parameters: {
        type: "object",
        properties: {
          preferred_time: {
            type: "string",
          },
        },
      },
    },
    {
      type: "function",
      name: "end_call",
      description:
        "Use when the conversation is complete, the lead wants to go, it is a wrong number, or they asked to stop contact.",
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
              "no_answer",
            ],
          },
          summary: {
            type: "string",
          },
        },
        required: ["reason"],
      },
    },
  ];
}

// ─────────────────────────────────────────────────────────────────────────────
// Tool Handler
// ─────────────────────────────────────────────────────────────────────────────

async function handleToolCall(
  name,
  args,
  callId,
  { lead, realtorProfile, callSid, calendlyLink, openAiWs, twilioWs, leadId }
) {
  const ack = (output) => {
    safeSend(openAiWs, {
      type: "conversation.item.create",
      item: {
        type: "function_call_output",
        call_id: callId,
        output: JSON.stringify(output),
      },
    });

    safeSend(openAiWs, {
      type: "response.create",
      response: {
        modalities: ["audio"],
      },
    });
  };

  if (name === "qualify_lead") {
    await callBase44("qualify_lead_from_call", {
      lead_id: leadId,
      ...args,
    }).catch((err) => {
      fastify.log.warn(`qualify_lead_from_call failed: ${err.message}`);
    });

    ack({
      success: true,
    });

    return;
  }

  if (name === "book_appointment") {
    await callBase44("send_calendly_sms", {
      lead_id: leadId,
      preferred_time: args.preferred_time || "",
      calendly_link: calendlyLink,
    }).catch((err) => {
      fastify.log.warn(`send_calendly_sms failed: ${err.message}`);
    });

    ack({
      success: true,
      message:
        "Tell the lead naturally: I just sent you a link to book a time that works for you.",
    });

    return;
  }

  if (name === "transfer_to_agent") {
    try {
      await callBase44("log_transfer", {
        lead_id: leadId,
        reason: args.reason || "",
        summary: args.summary || "",
      }).catch((err) => {
        fastify.log.warn(`log_transfer failed: ${err.message}`);
      });

      const transferTwiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Say voice="Polly.Joanna">Please hold while I connect you with the team.</Say>
  <Dial timeout="20" answerOnBridge="true">${escapeXml(
    AGENT_TRANSFER_NUMBER
  )}</Dial>
  <Say voice="Polly.Joanna">Sorry, the team is not available right now. We will follow up with you shortly.</Say>
  <Hangup/>
</Response>`;

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

      if (!res.ok) {
        const text = await res.text();
        throw new Error(`Twilio transfer failed (${res.status}): ${text}`);
      }

      ack({
        success: true,
        message: "Transfer initiated.",
      });
    } catch (err) {
      fastify.log.error(`Transfer failed: ${err.message}`);

      ack({
        success: false,
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
      message: "End the call politely and briefly.",
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
    error: `Unknown tool: ${name}`,
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// System Prompt
// ─────────────────────────────────────────────────────────────────────────────

function buildSystemPrompt(lead, realtorProfile) {
  const isaName = realtorProfile?.isa_name || "Emma";
  const teamName = realtorProfile?.display_name || "The Fugate Team";
  const firstName = lead?.name?.split(" ")[0] || "there";
  const source = lead?.source || "your inquiry";
  const leadType = lead?.lead_type || "buyer";

  const property = lead?.property_address
    ? `They originally inquired about ${lead.property_address}${
        lead.property_price ? ` listed at ${lead.property_price}` : ""
      }.`
    : "";

  const inquiryMessage = lead?.inquiry_message
    ? `Their original message was: "${lead.inquiry_message}".`
    : "";

  return `
You are ${isaName}, a friendly and professional real estate ISA with ${teamName}.
You are on a live outbound phone call with ${firstName}, who came in from ${source}.
Lead type: ${leadType}.
${property}
${inquiryMessage}

Your job is to have a natural, helpful conversation and determine how the team can help.

Start the call like a normal person:
"Hi, is this ${firstName}? This is ${isaName} with ${teamName}. I was following up on your real estate inquiry. Did I catch you at an okay time?"

Call goals:
First, confirm whether now is an okay time.
Then ask whether they are buying, selling, or just browsing.
Ask what area they are interested in.
Ask their timeline.
If they are buying, ask whether they have spoken with a lender or are pre-approved.
Ask whether they are already working with another agent.
If they are engaged, qualified, or ask for a person, offer to connect them with Daniel now.
If they prefer scheduling, use book_appointment.
Use qualify_lead once you collect useful qualification details.

Voice rules:
Speak in short, natural sentences.
Ask one question at a time.
Do not mention pressing buttons.
Do not sound like a phone tree.
Do not use bullet points, markdown, or lists.
Do not over-explain.
Be honest if asked whether you are AI: say you are a virtual assistant helping the team follow up quickly.
If the lead is busy, ask whether there is a better time and then end politely.
If they say stop, unsubscribe, remove me, wrong number, or do not call, apologize and use end_call immediately.
Do not give legal, tax, or loan advice. Refer those questions to Daniel, Sarah, or the lender.
Keep the tone calm, clear, professional, and conversational.
`.trim();
}

// ─────────────────────────────────────────────────────────────────────────────
// Start Server
// ─────────────────────────────────────────────────────────────────────────────

const PORT = Number(process.env.PORT || 3000);

fastify.listen({ port: PORT, host: "0.0.0.0" }, (err) => {
  if (err) {
    fastify.log.error(err);
    process.exit(1);
  }

  fastify.log.info(`Voice bridge running on port ${PORT}`);
});
