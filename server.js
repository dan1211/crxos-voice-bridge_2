fastify.get("/media-stream", { websocket: true }, async (connection, request) => {
  const twilioWs = connection.socket;

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

  twilioWs.on("message", async (raw) => {
    const msg = JSON.parse(raw);

    // ── Start event: extract lead_id and initialize everything ──────────────
    if (msg.event === "start") {
      streamSid = msg.start.streamSid;
      callSid = msg.start.callSid;
      leadId = msg.start.customParameters?.lead_id || request.query.lead_id || null;

      fastify.log.info(`Stream started — lead_id: ${leadId}, call_sid: ${callSid}`);

      if (!leadId) {
        fastify.log.error("No lead_id — closing");
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
        fastify.log.info(`Lead loaded: ${lead?.name}`);
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
        fastify.log.info("OpenAI WS opened");
        openAiWs.send(JSON.stringify({
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
        }));
      });

      openAiWs.on("message", async (aiRaw) => {
        const aiMsg = JSON.parse(aiRaw);

        if (aiMsg.type === "session.updated") {
          fastify.log.info("OpenAI session ready");
          sessionReady = true;
          for (const chunk of audioQueue) openAiWs.send(chunk);
          audioQueue.length = 0;
          openAiWs.send(JSON.stringify({ type: "response.create" }));
        }

        if (aiMsg.type === "response.audio.delta" && streamSid && twilioWs.readyState === 1) {
          twilioWs.send(JSON.stringify({
            event: "media",
            streamSid,
            media: { payload: aiMsg.delta },
          }));
        }

        if (aiMsg.type === "response.function_call_arguments.done") {
          fastify.log.info(`Tool call: ${aiMsg.name}`);
          await handleToolCall(
            aiMsg.name,
            JSON.parse(aiMsg.arguments || "{}"),
            aiMsg.call_id,
            { lead,
