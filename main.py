import os
import json
import base64
import asyncio
import websockets
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse
from fastapi.websockets import WebSocketDisconnect
from twilio.twiml.voice_response import VoiceResponse, Connect
from twilio.rest import Client
from dotenv import load_dotenv

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
PORT = int(os.getenv("PORT", 5050))
TEMPERATURE = float(os.getenv("TEMPERATURE", 0.6))

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

MANAGERS = {
    "anthony": "+13673809016",
    "martin": "+13673809016",
    "jessica": "+13673809016"
}

print("========== VARIABLES D'ENVIRONNEMENT ==========")
print(f"OPENAI_API_KEY : {'OK' if OPENAI_API_KEY else 'MANQUANTE'}")
print(f"PORT : {PORT}")
print("===============================================")

SYSTEM_MESSAGE = """
Tu es un assistant vocal professionnel et poli pour une entreprise de gestion locative au Québec.
Tu parles exclusivement en français québécois, de façon claire, calme et professionnelle.

Règles importantes :
- Tu réponds aux appels des locataires.
- Les gestionnaires sont :
  • Anthony : gestionnaire principal, responsable de la maintenance et de la supervision.
  • Martin John Wheeler : loyers, visites, plaintes, location, état des lieux.
  • Jessica Gilbert : loyers, visites, plaintes, location, état des lieux.
- Si la demande concerne une urgence de maintenance (fuite d’eau, pas d’électricité, chauffage en panne, etc.), priorise Anthony.
- Pour les questions de loyer, bail, visite ou plainte, oriente vers Martin ou Jessica.
- Tu peux créer un ticket de maintenance verbalement et confirmer que c’est enregistré.
- Si la demande est trop complexe ou si le locataire insiste, utilise l’outil transfer_to_manager pour le transférer.
- Sois concis. Ne parle pas trop longtemps.
"""

app = FastAPI()

@app.get("/")
async def index():
    return {"message": "Rental Voice Agent is running"}

@app.api_route("/incoming-call", methods=["GET", "POST"])
async def handle_incoming_call(request: Request):
    print(">>> /incoming-call reçu")
    response = VoiceResponse()

    # TwiML minimal pour que le WebSocket s'ouvre correctement
    connect = Connect()
    connect.stream(url="wss://rental-voice-agent-production.up.railway.app/media-stream")
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
        call_sid = None
        session_ready = False

        async def receive_from_twilio():
            nonlocal stream_sid, call_sid
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
                        call_sid = data["start"].get("callSid")
                        print(f"Stream started: {stream_sid}")
                        print(f"Call SID: {call_sid}")
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
                        print(">>> Session ready, sending greeting...")
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

                    if event_type == "response.function_call_arguments.done":
                        try:
                            function_name = response.get("name")
                            arguments = json.loads(response.get("arguments", "{}"))
                            print(f">>> Function call: {function_name}")
                            print(f">>> Arguments: {arguments}")

                            if function_name == "transfer_to_manager":
                                manager = arguments.get("manager", "anthony")
                                reason = arguments.get("reason", "")
                                print(f">>> Transfert demandé vers {manager} | Raison: {reason}")

                                if call_sid:
                                    success = await transfer_call(call_sid, manager)
                                    print(f">>> Résultat transfert: {success}")
                                else:
                                    print(">>> ERREUR: call_sid est None")
                        except Exception as e:
                            print(f"Erreur outil: {e}")

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
            "tools": [
                {
                    "type": "function",
                    "name": "transfer_to_manager",
                    "description": "Transférer l'appel vers un gestionnaire. Utilise cette fonction pour les urgences maintenance (Anthony) ou quand le locataire veut parler à un humain.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "manager": {
                                "type": "string",
                                "enum": ["anthony", "martin", "jessica"],
                                "description": "Le gestionnaire vers qui transférer"
                            },
                            "reason": {
                                "type": "string",
                                "description": "Raison du transfert"
                            }
                        },
                        "required": ["manager"]
                    }
                }
            ],
            "tool_choice": "auto",
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
    await asyncio.sleep(0.4)
    await openai_ws.send(json.dumps({
        "type": "conversation.item.create",
        "item": {
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": (
                    "Dis uniquement cette phrase au locataire, sans rien ajouter avant ni après : "
                    "Bonjour, je suis l’assistant de gestion locative. Cet appel peut être enregistré. "
                    "Comment puis-je vous aider ?"
                )
            }]
        }
    }))
    await openai_ws.send(json.dumps({"type": "response.create"}))
    print(">>> Greeting sent")

async def transfer_call(call_sid: str, manager: str):
    to_number = MANAGERS.get(manager.lower())
    if not to_number:
        print(f"Gestionnaire inconnu: {manager}")
        return False

    try:
        print(f">>> Transfert de {call_sid} vers {manager} ({to_number})")
        twilio_client.calls(call_sid).update(
            twiml=f"""
            <Response>
                <Dial>{to_number}</Dial>
            </Response>
            """
        )
        return True
    except Exception as e:
        print(f"Erreur lors du transfert: {e}")
        return False

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)