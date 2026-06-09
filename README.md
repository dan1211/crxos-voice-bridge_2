# CRX.OS Voice Bridge — Deployment Guide

## What This Does
Bridges Twilio Media Streams to OpenAI Realtime API so your ISA can have
live, natural voice conversations with leads instead of IVR press-1 flows.

```
Twilio Call → Base44 (voice_answer) → <Stream> TwiML
                                              ↓
                                    THIS SERVER (Railway)
                                         ↓       ↑
                                  OpenAI Realtime API
                                         ↓
                              Base44 REST (qualify, log, SMS)
```

---

## Step 1 — Deploy to Railway

1. Push this folder to a GitHub repo (e.g. `crxos-voice-bridge`)
2. Go to railway.app → New Project → Deploy from GitHub repo
3. Select your repo — Railway auto-detects Node and runs `npm start`
4. Go to Settings → Networking → Generate Domain
5. Copy your Railway URL (e.g. `https://crxos-voice-bridge.up.railway.app`)

---

## Step 2 — Set Environment Variables in Railway

In Railway dashboard → Variables, add:

| Variable | Value |
|---|---|
| `OPENAI_API_KEY` | Your OpenAI key |
| `BASE44_FUNCTION_URL` | Your Base44 function URL |
| `AGENT_TRANSFER_NUMBER` | e.g. `+19105551234` |
| `TWILIO_ACCOUNT_SID` | From Twilio console |
| `TWILIO_API_KEY_SID` | From Twilio API Keys |
| `TWILIO_API_KEY_SECRET` | From Twilio API Keys |
| `PORT` | `3000` |

---

## Step 3 — Add Two New Actions to Your Base44 Server

Add these action handlers to your existing Base44 Deno function:

### `get_lead_context`
Returns lead + realtorProfile so the bridge can build its system prompt.

```javascript
if (action === "get_lead_context") {
  const lead = await sr.entities.ISALead.get(payload.lead_id);
  if (!lead) return Response.json({ error: "Lead not found" }, { status: 404 });
  const realtorProfile = await getRealtorProfile(sr, lead.realtor_profile_id);
  return Response.json({ lead, realtorProfile });
}
```

### `qualify_lead_from_call`
Saves qualification data collected during the live call.

```javascript
if (action === "qualify_lead_from_call") {
  const lead = await sr.entities.ISALead.get(payload.lead_id);
  if (!lead) return Response.json({ error: "Lead not found" }, { status: 404 });
  const realtorProfile = await getRealtorProfile(sr, lead.realtor_profile_id);
  await sr.entities.ISALead.update(lead.id, {
    lead_type: payload.lead_type || lead.lead_type,
    status: "Responding",
    notes: [lead.notes, `[Live Call] Timeline: ${payload.timeline || "N/A"} | Budget: ${payload.budget || "N/A"} | Pre-approved: ${payload.pre_approved ?? "unknown"} | Working with agent: ${payload.working_with_agent ?? "unknown"}${payload.notes ? " | " + payload.notes : ""}`].filter(Boolean).join("\n\n"),
  });
  await logToFUB(lead.fub_person_id,
    `[ISA Live Call — Qualification]\nType: ${payload.lead_type}\nTimeline: ${payload.timeline || "N/A"}\nBudget: ${payload.budget || "N/A"}\nPre-approved: ${payload.pre_approved ?? "unknown"}\nNotes: ${payload.notes || ""}`,
    realtorProfile
  );
  return Response.json({ success: true });
}
```

### `send_calendly_sms`
Fires after the AI tells the lead they'll receive a booking link.

```javascript
if (action === "send_calendly_sms") {
  const lead = await sr.entities.ISALead.get(payload.lead_id);
  if (!lead) return Response.json({ error: "Lead not found" }, { status: 404 });
  const realtorProfile = await getRealtorProfile(sr, lead.realtor_profile_id);
  const link = payload.calendly_link || realtorProfile?.calendly_link || "";
  const firstName = lead.name?.split(" ")[0] || "there";
  if (lead.phone && link) {
    await sendSMS(lead.phone,
      `Hi ${firstName}, here's the link to schedule a call with the team: ${link}`,
      realtorProfile
    );
  }
  await logToFUB(lead.fub_person_id, `[ISA Live Call — Calendly SMS sent]\n${link}`, realtorProfile);
  return Response.json({ success: true });
}
```

### `log_transfer`
Logs when the AI initiates a warm transfer.

```javascript
if (action === "log_transfer") {
  const lead = await sr.entities.ISALead.get(payload.lead_id);
  if (!lead) return Response.json({ error: "Lead not found" }, { status: 404 });
  const realtorProfile = await getRealtorProfile(sr, lead.realtor_profile_id);
  await sr.entities.ISALead.update(lead.id, { status: "Handed Off", is_hot: true });
  await logToFUB(lead.fub_person_id,
    `[ISA Live Call — Warm Transfer]\nReason: ${payload.reason || "Lead requested agent"}`,
    realtorProfile
  );
  return Response.json({ success: true });
}
```

### `close_call`
Called when the AI ends the call.

```javascript
if (action === "close_call") {
  const lead = await sr.entities.ISALead.get(payload.lead_id);
  if (!lead) return Response.json({ error: "Lead not found" }, { status: 404 });
  const realtorProfile = await getRealtorProfile(sr, lead.realtor_profile_id);
  const statusMap = {
    completed: "Responding",
    not_interested: "Dead",
    dnc_requested: "Dead",
    no_answer: "New",
  };
  await sr.entities.ISALead.update(lead.id, {
    status: statusMap[payload.reason] || "Responding",
    last_contact_date: new Date().toISOString(),
  });
  await logToFUB(lead.fub_person_id,
    `[ISA Live Call — Ended]\nReason: ${payload.reason}`,
    realtorProfile
  );
  return Response.json({ success: true });
}
```

### `voice_call_ended`
Called when the WebSocket closes (cleanup/logging).

```javascript
if (action === "voice_call_ended") {
  const lead = await sr.entities.ISALead.get(payload.lead_id);
  if (!lead) return Response.json({ success: true });
  const realtorProfile = await getRealtorProfile(sr, lead.realtor_profile_id);
  await logToFUB(lead.fub_person_id,
    `[ISA Live Call — Stream Closed]\nCall SID: ${payload.call_sid || "unknown"}`,
    realtorProfile
  );
  return Response.json({ success: true });
}
```

---

## Step 4 — Update voice_answer in Base44

Replace your current `voice_answer` TwiML with a stream pointing to Railway:

```javascript
if (action === "voice_answer") {
  const lead = await sr.entities.ISALead.get(payload.lead_id);
  if (!lead) {
    return new Response(
      `<?xml version="1.0" encoding="UTF-8"?><Response><Hangup/></Response>`,
      { headers: { "Content-Type": "text/xml" } }
    );
  }

  const BRIDGE_URL = "wss://YOUR-RAILWAY-URL.up.railway.app"; // ← update this
  const callSid = payload.CallSid || "";

  const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${BRIDGE_URL}/media-stream?lead_id=${encodeURIComponent(lead.id)}&call_sid=${encodeURIComponent(callSid)}"/>
  </Connect>
</Response>`;

  // Store call SID on the lead for mid-call transfers
  await sr.entities.ISALead.update(lead.id, {
    current_call_sid: callSid,
    status: "Contacted",
    touch_number: (lead.touch_number || 0) + 1,
    last_contact_date: new Date().toISOString(),
  });

  return new Response(twiml, { headers: { "Content-Type": "text/xml" } });
}
```

> Make sure `current_call_sid` is a field on your ISALead entity in Base44.

---

## Step 5 — Add to Twilio Number Settings

In Twilio Console → Phone Numbers → your number:
- Voice → A call comes in → Webhook → `https://YOUR-BASE44-URL?action=voice_answer`
- HTTP Method: POST

That's it. Twilio calls Base44, Base44 returns the `<Stream>` TwiML pointing
to Railway, Railway handles the live AI conversation.

---

## Testing

Call your Twilio number. You should hear the AI greet the lead within ~2 seconds.

To test locally before Railway:
```bash
npm install
npm run dev
# Then use ngrok to expose port 3000
npx ngrok http 3000
# Use the ngrok WSS URL in your voice_answer TwiML
```
