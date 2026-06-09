/**
 * CRX.OS ISA — Live Voice Bridge Server
 * Bridges Twilio Media Streams ↔ OpenAI Realtime API
 *
 * ENV VARS (Railway dashboard):
 *   OPENAI_API_KEY
 *   BASE44_FUNCTION_URL    https://isa-dashboard.base44.app/api/functions/isaTrafficController
 *   BASE44_API_KEY         from Base44 → Settings → API Keys
 *   AGENT_TRANSFER_NUMBER  e.g. +19105551234
 *   TWILIO_ACCOUNT_SID
 *   TWILIO_API_KEY_SID
 *   TWILIO_API_KEY_SECRET
 *   PORT                   3000
 */

import Fastify from "fastify";
import FastifyWS from "@fastify/websocket";

const fastify = Fastify({ logger: true });
fastify.register(FastifyWS);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const BASE44_URL = process.env.BASE44_FUNCTION_URL;
const BASE44_API_KEY = process.env.BASE44_API_KEY;
const AGENT_TRANSFER_NUMBER = process.env.AGENT_TRANSFER_NUMBER;
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_API_KEY_SID = process.env.TWILIO_API_KEY_SID;
const TWILIO_API_KEY_SECRET = process.env.TWILIO_API_KEY_SECRET;

// ─── Base44 Helper ─────────────────────────────────────────────────────────────
async function callBase44(action, body) {
  const res = await fetch(`${BASE44_URL}?action=${action}`, {
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

// ─── Health Check ──────────────────────────────────────────────────────────────
fastify.get("/", async () => ({ status: "CRX.OS Voice Bridge running" }));

// ─── WebSocket: Media Stream Bridge ───────────────────────────────────────────
fastify.register(async (fastify) => {
  fastify.get("/media-stream", { websocket: true }, async (connection, request) => {
    const twilioWs = connection.socket;

    // State — populated when we receive the Twilio "start" event
    let leadId = null;
    let callSid = null;
    let lead = null;
    let realtorProfile = null;
    let systemPrompt = "";
    let calendlyLink = "";
    let openAiWs = null;
    let streamSid = "";
    let sessionReady = false;
    const audioQueue = [];

    fastify.log.info("WebSocket connection opened — waiting for Twilio start event");

    twilioWs.on("message", async (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw);
      } catch {
        return;
      }

      // ── connected event (Twilio sends this first, before start) ────────────
      if (msg.event === "connected") {
        fastify.log.info("Twilio connected event received");
        return;
      }

      // ── start event: extract lead_id from customParameters ─────────────────
      if (msg.event === "start") {
        streamSid = msg.start.streamSid;
        callSid = msg.start.callSid;

        // Primary: customParameters (set via <Parameter> tags in TwiML)
        // Fallback: query params (in case URL params work in some environments)
        leadId =
          msg.start.customParameters?.lead_id ||
          request.query?.lead_id ||
          null;

        fastify.log.info(
          `Twilio start — streamSid: ${streamSid}, callSid: ${callSid}, lead_id: ${leadId}`
        );
        fastify.log.info(
          `customParameters: ${JSON.stringify(msg.start.customParameters)}`
        );

        if (!leadId) {
          fastify.log.error("No lead_id found — closing connection");
          twilioWs.close();
          return;
        }

        // Load lead context from Base44
        try {
          const data = await callBase44("get_lead_context", { lead_id: leadId });
          lead = data.lead;
          realtorProfile = data.realtorProfile;
          calendlyLink = realtorProfile?.calendly_link || "";
          systemPrompt = buildSystemPrompt(lead, realtorProfile);
          fastify.log.info(`Lead loaded: ${lead?.name} (${leadId})`);
        } catch (err) {
          fastify.log.error("Failed to load lead context:", err.message);
          twilioWs.close();
          return;
        }

        // Open OpenAI Realtime session
        openAiWs = new WebSocket(
          "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01",
          [
            "realtime",
            `openai-insecure-api-key.${OPENAI_API_KEY}`,
            "openai-beta.realtime-v1",
          ]
        );

        openAiWs.on("open", () => {
          fastify.log.info("OpenAI Realtime WS opened");
          openAiWs.send(
            JSON.stringify({
              type: "session.update",
              session: {
                turn_detection: { type: "server_vad" },
                input_audio_format: "g711_ulaw",
                output_audio_format: "g711_ulaw",
                voice: "coral",
                instructions: systemPrompt,
                tools: getTools(),
                tool_choice: "auto",
                modalities: ["text", "audio"],
              },
            })
          );
        });

        openAiWs.on("message", async (aiRaw) => {
          let aiMsg;
          try {
            aiMsg = JSON.parse(aiRaw);
          } catch {
            return;
          }

          // Session ready — flush buffered audio and trigger greeting
          if (aiMsg.type === "session.updated") {
            fastify.log.info("OpenAI session ready — triggering greeting");
            sessionReady = true;
            for (const chunk of audioQueue) openAiWs.send(chunk);
            audioQueue.length = 0;
            openAiWs.send(JSON.stringify({ type: "response.create" }));
          }

          // Stream AI audio back to Twilio
          if (
            aiMsg.type === "response.audio.delta" &&
            streamSid &&
            twilioWs.readyState === 1
          ) {
            twilioWs.send(
              JSON.stringify({
                event: "media",
                streamSid,
                media: { payload: aiMsg.delta },
              })
            );
          }

          // Tool call
          if (aiMsg.type === "response.function_call_arguments.done") {
            fastify.log.info(`Tool call: ${aiMsg.name}`);
            await handleToolCall(
              aiMsg.name,
              JSON.parse(aiMsg.arguments || "{}"),
              aiMsg.call_id,
              { lead, realtorProfile, callSid, calendlyLink, openAiWs, twilioWs, leadId }
            );
          }
        });

        openAiWs.on("close", () => {
          fastify.log.info(`OpenAI WS closed for lead: ${leadId}`);
          if (twilioWs.readyState === 1) twilioWs.close();
          callBase44("voice_call_ended", { lead_id: leadId, call_sid: callSid }).catch(() => {});
        });

        openAiWs.on("error", (err) =>
          fastify.log.error("OpenAI WS error:", err.message)
        );

        return;
      }

      // ── media event: forward audio to OpenAI ───────────────────────────────
      if (msg.event === "media") {
        if (!openAiWs) return; // not ready yet
        const audioChunk = JSON.stringify({
          type: "input_audio_buffer.append",
          audio: msg.media.payload,
        });
        if (sessionReady && openAiWs.readyState === 1) {
          openAiWs.send(audioChunk);
        } else {
          audioQueue.push(audioChunk);
        }
        return;
      }

      // ── stop event ─────────────────────────────────────────────────────────
      if (msg.event === "stop") {
        fastify.log.info(`Twilio stream stopped for lead: ${leadId}`);
        if (openAiWs?.readyState === 1) openAiWs.close();
      }
    });

    twilioWs.on("close", () => {
      fastify.log.info(`Twilio WS closed for lead: ${leadId}`);
      if (openAiWs?.readyState === 1) openAiWs.close();
    });

    twilioWs.on("error", (err) =>
      fastify.log.error("Twilio WS error:", err.message)
    );
  });
});

// ─── Tool Definitions ──────────────────────────────────────────────────────────
function getTools() {
  return [
    {
      type: "function",
      name: "qualify_lead",
      description:
        "Save qualification details collected during the call. Call this as soon as you have the key facts — don't wait until the end.",
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
        "Use when the lead asks to speak to a person, is ready to make an offer, or is clearly a hot qualified lead.",
      parameters: {
        type: "object",
        properties: {
          reason: { type: "string" },
        },
        required: ["reason"],
      },
    },
    {
      type: "function",
      name: "book_appointment",
      description:
        "Use when the lead wants to schedule a call. Send them the Calendly link via SMS.",
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
        "Use when the conversation is complete, the lead wants to go, or they asked to stop contact.",
      parameters: {
        type: "object",
        properties: {
          reason: {
            type: "string",
            enum: ["completed", "not_interested", "dnc_requested", "no_answer"],
          },
        },
        required: ["reason"],
      },
    },
  ];
}

// ─── Tool Handler ──────────────────────────────────────────────────────────────
async function handleToolCall(
  name,
  args,
  callId,
  { lead, realtorProfile, callSid, calendlyLink, openAiWs, twilioWs, leadId }
) {
  const ack = (output) => {
    openAiWs.send(
      JSON.stringify({
        type: "conversation.item.create",
        item: {
          type: "function_call_output",
          call_id: callId,
          output: JSON.stringify(output),
        },
      })
    );
    openAiWs.send(JSON.stringify({ type: "response.create" }));
  };

  if (name === "qualify_lead") {
    await callBase44("qualify_lead_from_call", { lead_id: leadId, ...args }).catch(() => {});
    ack({ success: true });
  }

  if (name === "book_appointment") {
    await callBase44("send_calendly_sms", {
      lead_id: leadId,
      preferred_time: args.preferred_time || "",
      calendly_link: calendlyLink,
    }).catch(() => {});
    ack({
      success: true,
      message: `Tell the lead: "I just texted you a link to book a time that works for you."`,
    });
  }

  if (name === "transfer_to_agent") {
    try {
      const authHeader =
        "Basic " +
        Buffer.from(`${TWILIO_API_KEY_SID}:${TWILIO_API_KEY_SECRET}`).toString("base64");
      await fetch(
        `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${callSid}.json`,
        {
          method: "POST",
          headers: {
            Authorization: authHeader,
            "Content-Type": "application/x-www-form-urlencoded",
          },
          body: new URLSearchParams({
            Twiml: `<Response><Say voice="Polly.Joanna">Please hold while I connect you with the team.</Say><Dial timeout="20">${AGENT_TRANSFER_NUMBER}</Dial></Response>`,
          }),
        }
      );
      await callBase44("log_transfer", { lead_id: leadId, reason: args.reason }).catch(() => {});
      ack({ success: true });
    } catch (err) {
      fastify.log.error("Transfer failed:", err.message);
      ack({ success: false, error: err.message });
    }
  }

  if (name === "end_call") {
    await callBase44("close_call", { lead_id: leadId, reason: args.reason }).catch(() => {});
    ack({ success: true });
    setTimeout(() => {
      if (twilioWs.readyState === 1) twilioWs.close();
    }, 4000);
  }
}

// ─── System Prompt Builder ─────────────────────────────────────────────────────
function buildSystemPrompt(lead, realtorProfile) {
  const isaName = realtorProfile?.isa_name || "Emma";
  const teamName = realtorProfile?.display_name || "the team";
  const firstName = lead?.name?.split(" ")[0] || "there";
  const source = lead?.source || "your inquiry";
  const property = lead?.property_address
    ? `They originally inquired about ${lead.property_address}${
        lead.property_price ? ` listed at ${lead.property_price}` : ""
      }.`
    : "";

  return `
You are ${isaName}, a friendly and professional real estate ISA (Inside Sales Agent) with ${teamName}.
You are on a live phone call with ${firstName}, who came in from ${source}. ${property}

YOUR GOALS FOR THIS CALL (in order):
1. Greet them warmly and confirm this is a good time to talk.
2. Ask if they are looking to buy or sell.
3. Understand their timeline and general budget/price range.
4. For buyers: ask if they have spoken with a lender or are pre-approved.
5. Ask if they are currently working with another agent.
6. If they are a strong lead: offer to connect them with the agent now (transfer) or schedule a call (book_appointment).
7. Use qualify_lead to save what you learn as you go — don't wait until the end.

VOICE RULES:
- You are speaking out loud on a phone call. Keep sentences short and conversational.
- Never use bullet points, markdown, or lists — you are speaking, not writing.
- Pause naturally. Ask one question at a time.
- If they seem hesitant, don't push — stay warm and helpful.
- If they say stop, unsubscribe, or do not call: use end_call with reason "dnc_requested" immediately.
- If they ask to speak to a person or say they are ready to move forward: use transfer_to_agent.
- If they want to schedule a call: use book_appointment and tell them you will text them a link.
- Do not give legal, loan, or tax advice. Direct those questions to the agent or lender.
- Keep the tone calm, clear, and professional — like a helpful team member, not a salesperson.

TEAM INFO:
- ISA Name: ${isaName}
- Team: ${teamName}
- Lead name: ${firstName}
- Lead source: ${source}
`.trim();
}

// ─── Start Server ──────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
fastify.listen({ port: PORT, host: "0.0.0.0" }, (err) => {
  if (err) {
    fastify.log.error(err);
    process.exit(1);
  }
  fastify.log.info(`Voice bridge running on port ${PORT}`);
});
