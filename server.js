/**
 * CRX.OS ISA — Live Voice Bridge Server
 * Bridges Twilio Media Streams ↔ OpenAI Realtime API
 * Deploy this on Railway. Your Base44 server stays unchanged.
 *
 * ENV VARS NEEDED (set in Railway dashboard):
 *   OPENAI_API_KEY
 *   BASE44_FUNCTION_URL       ← your existing Base44 function URL
 *   AGENT_TRANSFER_NUMBER     ← e.g. +19105551234
 *   TWILIO_ACCOUNT_SID
 *   TWILIO_API_KEY_SID
 *   TWILIO_API_KEY_SECRET
 */

import Fastify from "fastify";
import FastifyWS from "@fastify/websocket";

const fastify = Fastify({ logger: true });
fastify.register(FastifyWS);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const BASE44_URL = process.env.BASE44_FUNCTION_URL;
const AGENT_TRANSFER_NUMBER = process.env.AGENT_TRANSFER_NUMBER;
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID;
const TWILIO_API_KEY_SID = process.env.TWILIO_API_KEY_SID;
const TWILIO_API_KEY_SECRET = process.env.TWILIO_API_KEY_SECRET;

// ─── Health Check ─────────────────────────────────────────────────────────────
fastify.get("/", async () => ({ status: "CRX.OS Voice Bridge running" }));

// ─── Twilio calls this first to get TwiML with the stream URL ─────────────────
// In Base44, your voice_answer action should now return this TwiML:
//
//   <Response>
//     <Connect>
//       <Stream url="wss://YOUR-RAILWAY-URL/media-stream?lead_id=XXX&call_sid={{CallSid}}"/>
//     </Connect>
//   </Response>
//
// Pass call_sid through so we can do mid-call transfers.

// ─── WebSocket: Media Stream Bridge ──────────────────────────────────────────
fastify.register(async (fastify) => {
  fastify.get("/media-stream", { websocket: true }, async (connection, request) => {
  const leadId = request.query.lead_id || null;
  const callSid = request.query.call_sid || "";

  fastify.log.info(`WebSocket connected — lead_id: ${leadId}, call_sid: ${callSid}`);

  if (!leadId) {
    fastify.log.error("No lead_id — closing");
    connection.socket.close();
    return;
  }

  const twilioWs = connection.socket;

    // Fetch lead + realtor profile from Base44
    let lead = null;
    let realtorProfile = null;
    let systemPrompt = "";
    let calendlyLink = "";

    try {
      const res = await fetch(`${BASE44_URL}?action=get_lead_context&lead_id=${leadId}`);
      const data = await res.json();
      lead = data.lead;
      realtorProfile = data.realtorProfile;
      calendlyLink = realtorProfile?.calendly_link || "";

      systemPrompt = buildSystemPrompt(lead, realtorProfile);
    } catch (err) {
      fastify.log.error("Failed to load lead context:", err);
      twilioWs.close();
      return;
    }

    // Open OpenAI Realtime session
    const openAiWs = new WebSocket(
      "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01",
      [
        "realtime",
        `openai-insecure-api-key.${OPENAI_API_KEY}`,
        "openai-beta.realtime-v1",
      ]
    );

    let streamSid = "";
    let sessionReady = false;
    const audioQueue = []; // buffer audio that arrives before session is ready

    // ── OpenAI session config on connect ─────────────────────────────────────
    openAiWs.on("open", () => {
      openAiWs.send(
        JSON.stringify({
          type: "session.update",
          session: {
            turn_detection: { type: "server_vad" },
            input_audio_format: "g711_ulaw",
            output_audio_format: "g711_ulaw",
            voice: "alloy",
            instructions: systemPrompt,
            tools: getTools(),
            tool_choice: "auto",
            modalities: ["text", "audio"],
          },
        })
      );
    });

    // ── OpenAI → Twilio (audio + tool handling) ───────────────────────────────
    openAiWs.on("message", async (raw) => {
      const msg = JSON.parse(raw);

      // Session ready — flush any buffered audio
      if (msg.type === "session.updated") {
        sessionReady = true;
        for (const chunk of audioQueue) {
          openAiWs.send(chunk);
        }
        audioQueue.length = 0;
        // Trigger opening greeting
        openAiWs.send(JSON.stringify({ type: "response.create" }));
      }

      // Stream AI audio back to Twilio
      if (
        msg.type === "response.audio.delta" &&
        streamSid &&
        twilioWs.readyState === 1
      ) {
        twilioWs.send(
          JSON.stringify({
            event: "media",
            streamSid,
            media: { payload: msg.delta },
          })
        );
      }

      // Tool call completed — handle it
      if (msg.type === "response.function_call_arguments.done") {
        await handleToolCall(
          msg.name,
          JSON.parse(msg.arguments || "{}"),
          msg.call_id,
          { lead, realtorProfile, callSid, calendlyLink, openAiWs, twilioWs, leadId }
        );
      }
    });

    // ── Twilio → OpenAI (inbound audio) ──────────────────────────────────────
    twilioWs.on("message", (raw) => {
      const msg = JSON.parse(raw);

      if (msg.event === "start") {
        streamSid = msg.start.streamSid;
        fastify.log.info(`Stream started: ${streamSid} for lead: ${leadId}`);
      }

      if (msg.event === "media") {
        const audioChunk = JSON.stringify({
          type: "input_audio_buffer.append",
          audio: msg.media.payload,
        });

        if (sessionReady && openAiWs.readyState === 1) {
          openAiWs.send(audioChunk);
        } else {
          audioQueue.push(audioChunk);
        }
      }

      if (msg.event === "stop") {
        fastify.log.info(`Stream stopped for lead: ${leadId}`);
        openAiWs.close();
      }
    });

    // ── Cleanup ───────────────────────────────────────────────────────────────
    openAiWs.on("close", () => {
      fastify.log.info(`OpenAI WS closed for lead: ${leadId}`);
      if (twilioWs.readyState === 1) twilioWs.close();

      // Log call end to Base44
      fetch(`${BASE44_URL}?action=voice_call_ended`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ lead_id: leadId, call_sid: callSid }),
      }).catch(() => {});
    });

    twilioWs.on("close", () => {
      if (openAiWs.readyState === 1) openAiWs.close();
    });

    openAiWs.on("error", (err) => fastify.log.error("OpenAI WS error:", err));
    twilioWs.on("error", (err) => fastify.log.error("Twilio WS error:", err));
  });
});

// ─── Tool Definitions ─────────────────────────────────────────────────────────
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
          lead_type: {
            type: "string",
            enum: ["buyer", "seller"],
            description: "Whether they are buying or selling",
          },
          timeline: {
            type: "string",
            description: "When they want to buy/sell, e.g. 'within 3 months'",
          },
          budget: {
            type: "string",
            description: "Price range or budget, e.g. '$300k-$400k'",
          },
          pre_approved: {
            type: "boolean",
            description: "Whether buyer is pre-approved for a mortgage",
          },
          working_with_agent: {
            type: "boolean",
            description: "Whether they are already working with an agent",
          },
          notes: {
            type: "string",
            description: "Any other important details from the conversation",
          },
        },
        required: ["lead_type"],
      },
    },
    {
      type: "function",
      name: "transfer_to_agent",
      description:
        "Use this when the lead asks to speak to a person, says they are ready to make an offer, or is clearly a hot qualified lead ready to act now.",
      parameters: {
        type: "object",
        properties: {
          reason: {
            type: "string",
            description: "Why you are transferring this lead",
          },
        },
        required: ["reason"],
      },
    },
    {
      type: "function",
      name: "book_appointment",
      description:
        "Use this when the lead wants to schedule a call or meeting with the agent. Send them the Calendly link via SMS.",
      parameters: {
        type: "object",
        properties: {
          preferred_time: {
            type: "string",
            description: "What time they mentioned if any, e.g. 'tomorrow afternoon'",
          },
        },
      },
    },
    {
      type: "function",
      name: "end_call",
      description:
        "Use this when the conversation is complete, the lead wants to go, or they asked to stop contact.",
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

// ─── Tool Handler ─────────────────────────────────────────────────────────────
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
    await fetch(`${BASE44_URL}?action=qualify_lead_from_call`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ lead_id: leadId, ...args }),
    }).catch(() => {});
    ack({ success: true });
  }

  if (name === "book_appointment") {
    // Send Calendly link via SMS through Base44
    await fetch(`${BASE44_URL}?action=send_calendly_sms`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        lead_id: leadId,
        preferred_time: args.preferred_time || "",
        calendly_link: calendlyLink,
      }),
    }).catch(() => {});
    ack({
      success: true,
      message: `Tell the lead: "I just texted you a link to book a time that works for you."`,
    });
  }

  if (name === "transfer_to_agent") {
    try {
      // Inject warm transfer TwiML into the live call via Twilio REST
      const authHeader =
        "Basic " + Buffer.from(`${TWILIO_API_KEY_SID}:${TWILIO_API_KEY_SECRET}`).toString("base64");
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
      // Log transfer to Base44
      await fetch(`${BASE44_URL}?action=log_transfer`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ lead_id: leadId, reason: args.reason }),
      }).catch(() => {});
      ack({ success: true });
    } catch (err) {
      ack({ success: false, error: err.message });
    }
  }

  if (name === "end_call") {
    await fetch(`${BASE44_URL}?action=close_call`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ lead_id: leadId, reason: args.reason }),
    }).catch(() => {});
    ack({ success: true });
    // Hang up after a short delay so AI can say goodbye
    setTimeout(() => {
      if (twilioWs.readyState === 1) twilioWs.close();
    }, 4000);
  }
}

// ─── System Prompt Builder ────────────────────────────────────────────────────
function buildSystemPrompt(lead, realtorProfile) {
  const isaName = realtorProfile?.isa_name || "Emma";
  const teamName = realtorProfile?.display_name || "the team";
  const firstName = lead?.name?.split(" ")[0] || "there";
  const source = lead?.source || "your inquiry";
  const property = lead?.property_address
    ? `They originally inquired about ${lead.property_address}${lead.property_price ? ` listed at ${lead.property_price}` : ""}.`
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
- If they want to schedule a call: use book_appointment and tell them you'll text them a link.
- Do not give legal, loan, or tax advice. Direct those questions to the agent or lender.
- Keep the tone calm, clear, and professional — like a helpful team member, not a salesperson.

TEAM INFO:
- ISA Name: ${isaName}
- Team: ${teamName}
- Lead name: ${firstName}
- Lead source: ${source}
`.trim();
}

// ─── Start Server ─────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
fastify.listen({ port: PORT, host: "0.0.0.0" }, (err) => {
  if (err) {
    fastify.log.error(err);
    process.exit(1);
  }
  fastify.log.info(`Voice bridge running on port ${PORT}`);
});
