import os
import json
import base64
import asyncio
import websockets
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse
from fastapi.websockets import WebSocketDisconnect
from twilio.twiml.voice_response import VoiceResponse, Connect
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PORT = int(os.getenv("PORT", 5050))
TEMPERATURE = float(os.getenv("TEMPERATURE", 0.6))

print("========== VARIABLES D'ENVIRONNEMENT ==========")
print(f"OPENAI_API_KEY : {'OK' if OPENAI_API_KEY else 'MANQUANTE'}")
print(f"PORT : {PORT}")
print("===============================================")

SYSTEM_MESSAGE = """
Tu es un assistant vocal professionnel et poli pour une entreprise de gestion locative au Québec.
Tu parles exclusivement en français québécois, de façon claire, calme et professionnelle.
Les gestionnaires sont Anthony (maintenance), Martin John Wheeler et Jessica Gilbert.
Commence toujours par te présenter brièvement.
"""

app = FastAPI()

@app.get("/")
async def index():
    return {"message": "Rental Voice Agent is running"}

@app.api_route("/incoming-call", methods=["GET", "POST"])
async def handle_incoming_call(request: Request):
    print(">>> /incoming-call reçu")
    response = VoiceResponse()
    response.say(
        "Bonjour. Cet appel peut être enregistré pour des fins de qualité de service et de suivi. "
        "Si vous n’acceptez pas l’enregistrement, veuillez raccrocher. "
        "Un instant s’il vous plaît, je vous mets en communication avec notre assistant.",
        voice="Polly.Gabrielle",
        language="fr-CA"
    )
    response.pause(length=1)

    connect = Connect()
    # Mets ici l'URL de ton service Railway une fois qu'il sera en ligne
    connect.stream(url="wss://neuter-power-enduring.ngrok-free.dev/media-stream")
    response.append(connect)

    return HTMLResponse(content=str(response), media_type="application/xml")

@app.websocket("/media-stream")
async def handle_media_stream(websocket: WebSocket):
    print(">>> WebSocket /media-stream atteint !")
    await websocket.accept()
    print("Client connected")

    async with websockets.connect(
        f"wss://api.openai.com/v1/realtime?model=gpt-realtime&temperature={TEMPERATURE}",
        additional_headers={"Authorization": f"Bearer {OPENAI_API_KEY}"}
    ) as openai_ws:

        stream_sid = None
        session_ready = False

        async def receive_from_twilio():
            nonlocal stream_sid
            try:
                async for message in websocket.iter_text():
                    data = json.loads(message)
                    if data["event"] == "media" and openai_ws.state.name == "OPEN":
                        await openai_ws.send(json.dumps({
                            "type": "input_audio_buffer.append",
                            "audio": data["media"]["payload"]
                        }))
                    elif data["event"] == "start":
                        stream_sid = data["start"]["streamSid"]
                        print(f"Stream started: {stream_sid}")
            except WebSocketDisconnect:
                print("Client disconnected")
                if openai_ws.state.name == "OPEN":
                    await openai_ws.close()

        async def send_to_twilio():
            nonlocal session_ready
            try:
                async for openai_message in openai_ws:
                    response = json.loads(openai_message)
                    event_type = response.get("type")
                    print(f">>> OpenAI event: {event_type}")

                    if event_type == "session.updated" and not session_ready:
                        session_ready = True
                        print(">>> Session ready, sending initial greeting...")
                        await send_initial_conversation_item(openai_ws)

                    if event_type == "response.output_audio.delta" and "delta" in response:
                        audio_payload = base64.b64encode(
                            base64.b64decode(response["delta"])
                        ).decode("utf-8")
                        await websocket.send_json({
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {"payload": audio_payload}
                        })
            except Exception as e:
                print(f"Error in send_to_twilio: {e}")

        await initialize_session(openai_ws)
        await asyncio.gather(receive_from_twilio(), send_to_twilio())

async def initialize_session(openai_ws):
    session_update = {
        "type": "session.update",
        "session": {
            "type": "realtime",
            "instructions": SYSTEM_MESSAGE,
            "audio": {
                "input": {
                    "format": {"type": "audio/pcmu"},
                    "turn_detection": {"type": "server_vad"}
                },
                "output": {
                    "format": {"type": "audio/pcmu"},
                    "voice": "alloy"
                }
            }
        }
    }
    print(">>> Sending session.update...")
    await openai_ws.send(json.dumps(session_update))

async def send_initial_conversation_item(openai_ws):
    await asyncio.sleep(0.3)
    await openai_ws.send(json.dumps({
        "type": "conversation.item.create",
        "item": {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "Présente-toi brièvement en tant qu’assistant de gestion locative et demande comment tu peux aider."
            }]
        }
    }))
    await openai_ws.send(json.dumps({"type": "response.create"}))
    print(">>> response.create sent")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)