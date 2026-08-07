# voice-svc

The **Voice Service** is the bounded context responsible for **real-time voice and video communication** in the Harmony platform. It manages WebRTC signaling, operates a **Selective Forwarding Unit (SFU)** via `aiortc`, and maintains ephemeral session state in Redis. It is written in **Python 3.12** using **FastAPI** and follows **DDD + Clean Architecture** (Ports & Adapters).

---

## Table of contents

- [Overview](#overview)
- [Bounded context](#bounded-context)
- [Architecture](#architecture)
- [Folder structure](#folder-structure)
- [Domain layer](#domain-layer)
- [Application layer](#application-layer)
- [Infrastructure layer](#infrastructure-layer)
- [API layer](#api-layer)
- [WebRTC flows](#webrtc-flows)
- [SFU internals](#sfu-internals)
- [Kafka events](#kafka-events)
- [Database and state](#database-and-state)
- [Local development — localhost setup](#local-development--localhost-setup)
- [Testing strategy](#testing-strategy)
- [Environment variables](#environment-variables)
- [Related services](#related-services)

---

## Overview

| Property | Value |
|---|---|
| Language | Python 3.12 |
| Framework | FastAPI (ASGI — uvicorn) |
| Architecture | DDD + Clean Architecture (Ports & Adapters) |
| SFU library | `aiortc` (Python WebRTC) |
| Signaling transport | WebSocket (`/ws/voice/{channelId}`) |
| Session state | Redis (ephemeral, TTL 65s per heartbeat) |
| Media | SRTP over UDP (WebRTC) — never decoded by SFU |
| Database | PostgreSQL (voice session history only) |
| Messaging | Kafka (publishes voice session events) |
| WebSocket port | `8086` |
| UDP port range | `40000–49999` (media) |

---

## Bounded context

`voice-svc` owns **the real-time media layer** — session lifecycle, SFU routing, and participant management. It does not own channel metadata (that belongs to `community-svc`) or user profiles (that belongs to `user-svc`).

```
Client A joins voice channel
  └─► voice-svc (signaling WS + SFU)
        ├─► community-svc gRPC (verify channel exists)
        ├─► auth-svc JWKS (verify JWT)
        └─► ws-gateway Kafka event (notify members someone joined)

voice-svc ──► voice.session.started ──► ws-gateway (show speaking indicator)
          ──► voice.session.ended   ──► ws-gateway (remove participant)
          ──► voice.session.started ──► notification-svc
```

> **Rule:** voice-svc never reads community-svc's database. All channel verification goes through the `ChannelGrpcClient` port, which calls `community-svc` gRPC.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                           API layer                                   │
│  FastAPI WebSocket handler  ·  REST controllers  ·  Pydantic models  │
│                                                                       │
│   ┌──────────────────────────────────────────────────────────────┐   │
│   │                     Application layer                        │   │
│   │  JoinChannelUseCase  ·  LeaveChannelUseCase                  │   │
│   │  SfuOrchestrationService  ·  SessionService                  │   │
│   │  Ports: ISfuService  ISessionRepo  IEventPublisher           │   │
│   │         IChannelVerifier  ICacheService                      │   │
│   │                                                              │   │
│   │   ┌──────────────────────────────────────────────────────┐   │   │
│   │   │                  Domain layer                        │   │   │
│   │   │  VoiceSession (aggregate root)                       │   │   │
│   │   │  Participant (entity)  ·  MediaTrack (value object)  │   │   │
│   │   │  VoiceSessionStarted / VoiceSessionEnded (events)    │   │   │
│   │   │  No framework imports — pure Python dataclasses      │   │   │
│   │   └──────────────────────────────────────────────────────┘   │   │
│   └──────────────────────────────────────────────────────────────┘   │
│                                                                       │
│   Infrastructure                                                      │
│   ─────────────                                                       │
│   AiortcSfuAdapter  ·  RedisSessionRepo  ·  KafkaVoicePublisher      │
│   ChannelGrpcClient  ·  PgVoiceHistoryRepo  ·  RedisCacheAdapter     │
└──────────────────────────────────────────────────────────────────────┘
```

**Dependency rule:** all imports point inward. `domain/` has zero external dependencies. `application/` imports only `domain/`. `infrastructure/` imports both. `api/` imports all.

---

## Folder structure

```
voice-svc/
├── src/
│   ├── domain/
│   │   ├── voice_session/
│   │   │   ├── voice_session.py          # VoiceSession aggregate root
│   │   │   ├── voice_session_events.py   # VoiceSessionStarted, VoiceSessionEnded
│   │   │   └── session_repository.py     # ISessionRepository port (interface)
│   │   ├── participant/
│   │   │   ├── participant.py            # Participant entity
│   │   │   └── participant_status.py     # Enum: Active, Muted, Deafened
│   │   └── value_objects/
│   │       ├── media_track.py            # MediaTrack: trackId, kind (audio/video)
│   │       ├── channel_id.py             # Validated UUID
│   │       └── sdp_offer.py             # Validated SDP string value object
│   │
│   ├── application/
│   │   ├── commands/
│   │   │   ├── join_channel/
│   │   │   │   ├── join_channel_command.py      # dataclass: channel_id, user_id, sdp_offer
│   │   │   │   └── join_channel_handler.py      # JoinChannelHandler
│   │   │   ├── leave_channel/
│   │   │   │   ├── leave_channel_command.py
│   │   │   │   └── leave_channel_handler.py
│   │   │   ├── set_mute/
│   │   │   │   └── set_mute_handler.py
│   │   │   └── add_ice_candidate/
│   │   │       └── add_ice_candidate_handler.py
│   │   ├── services/
│   │   │   ├── sfu_orchestration_service.py     # coordinates SFU + session state
│   │   │   └── session_service.py               # session lifecycle helpers
│   │   └── ports/
│   │       ├── i_sfu_service.py                 # ISfuService ABC
│   │       ├── i_session_repo.py                # ISessionRepo ABC
│   │       ├── i_event_publisher.py             # IEventPublisher ABC
│   │       ├── i_channel_verifier.py            # IChannelVerifier ABC
│   │       └── i_cache_service.py               # ICacheService ABC
│   │
│   ├── infrastructure/
│   │   ├── sfu/
│   │   │   ├── aiortc_sfu_adapter.py            # implements ISfuService via aiortc
│   │   │   ├── sfu_router_manager.py            # manages Router per channelId
│   │   │   └── transport_factory.py             # creates WebRtcTransport objects
│   │   ├── persistence/
│   │   │   ├── redis_session_repo.py            # implements ISessionRepo
│   │   │   ├── pg_voice_history_repo.py         # PostgreSQL session history
│   │   │   └── redis_cache_adapter.py           # implements ICacheService
│   │   ├── messaging/
│   │   │   ├── kafka_voice_publisher.py         # implements IEventPublisher
│   │   │   └── kafka_config.py
│   │   └── grpc/
│   │       ├── channel_grpc_client.py           # implements IChannelVerifier
│   │       └── protos/
│   │           └── channel_service_pb2.py       # generated from channel_service.proto
│   │
│   ├── api/
│   │   ├── websocket/
│   │   │   ├── signaling_handler.py             # WS router — handles all signaling msgs
│   │   │   └── message_types.py                 # Pydantic models for WS message shapes
│   │   ├── rest/
│   │   │   ├── session_router.py                # GET /sessions/:channelId/participants
│   │   │   └── health_router.py                 # GET /health
│   │   └── middleware/
│   │       ├── jwt_middleware.py                # validate Bearer token via JWKS
│   │       └── rate_limit_middleware.py
│   │
│   ├── container.py                             # dependency injection wiring
│   └── main.py                                  # FastAPI app factory + lifespan
│
├── tests/
│   ├── unit/
│   │   ├── domain/
│   │   └── application/
│   └── integration/
│       ├── test_signaling.py
│       └── test_sfu_adapter.py
│
├── migrations/
│   └── 001_create_voice_history.sql
│
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml                               # dependencies + tool config
├── requirements.txt                             # pinned for production
├── requirements-dev.txt                         # dev/test extras
├── .env.example
└── README.md
```

---

## Domain layer

`src/domain/` — pure Python dataclasses, no framework imports.

### `VoiceSession` aggregate root

```python
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID, uuid4
from typing import List

@dataclass
class VoiceSession:
    id: UUID
    channel_id: UUID
    participants: List["Participant"] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.utcnow)
    _events: List[object] = field(default_factory=list, repr=False)

    @classmethod
    def create(cls, channel_id: UUID) -> "VoiceSession":
        session = cls(id=uuid4(), channel_id=channel_id)
        session._events.append(VoiceSessionStarted(
            session_id=session.id,
            channel_id=channel_id,
            occurred_at=session.started_at
        ))
        return session

    def add_participant(self, user_id: UUID) -> "Participant":
        if any(p.user_id == user_id for p in self.participants):
            raise DuplicateParticipantError(user_id)
        if len(self.participants) >= 25:
            raise ChannelFullError()
        p = Participant(id=uuid4(), user_id=user_id, session_id=self.id)
        self.participants.append(p)
        return p

    def remove_participant(self, user_id: UUID) -> None:
        self.participants = [p for p in self.participants if p.user_id != user_id]
        if not self.participants:
            self._events.append(VoiceSessionEnded(
                session_id=self.id,
                channel_id=self.channel_id,
                occurred_at=datetime.utcnow()
            ))

    def pull_events(self) -> List[object]:
        events, self._events = self._events, []
        return events

    @property
    def is_empty(self) -> bool:
        return len(self.participants) == 0
```

### `Participant` entity

```python
@dataclass
class Participant:
    id: UUID
    user_id: UUID
    session_id: UUID
    status: ParticipantStatus = ParticipantStatus.ACTIVE
    joined_at: datetime = field(default_factory=datetime.utcnow)
    audio_track_id: str | None = None
    video_track_id: str | None = None

    def mute(self) -> None:
        self.status = ParticipantStatus.MUTED

    def unmute(self) -> None:
        self.status = ParticipantStatus.ACTIVE

    def set_deafened(self, deafened: bool) -> None:
        self.status = ParticipantStatus.DEAFENED if deafened else ParticipantStatus.ACTIVE
```

### `MediaTrack` value object

```python
from enum import Enum

class TrackKind(str, Enum):
    AUDIO = "audio"
    VIDEO = "video"

@dataclass(frozen=True)
class MediaTrack:
    track_id: str
    kind: TrackKind
    producer_id: str   # aiortc Producer ID

    def __post_init__(self):
        if not self.track_id:
            raise ValueError("track_id must not be empty")
```

---

## Application layer

### Port interfaces (`src/application/ports/`)

```python
# i_sfu_service.py
from abc import ABC, abstractmethod

class ISfuService(ABC):
    @abstractmethod
    async def get_or_create_router(self, channel_id: str) -> str: ...

    @abstractmethod
    async def create_transport(self, channel_id: str, user_id: str) -> dict: ...

    @abstractmethod
    async def connect_transport(self, transport_id: str, dtls_params: dict) -> None: ...

    @abstractmethod
    async def create_producer(self, transport_id: str, rtp_params: dict) -> str: ...

    @abstractmethod
    async def create_consumer(self, transport_id: str, producer_id: str,
                               rtp_capabilities: dict) -> dict: ...

    @abstractmethod
    async def close_transport(self, transport_id: str) -> None: ...

    @abstractmethod
    async def destroy_router(self, channel_id: str) -> None: ...


# i_session_repo.py
class ISessionRepo(ABC):
    @abstractmethod
    async def save_session(self, session: VoiceSession) -> None: ...

    @abstractmethod
    async def find_session(self, channel_id: UUID) -> VoiceSession | None: ...

    @abstractmethod
    async def delete_session(self, channel_id: UUID) -> None: ...

    @abstractmethod
    async def add_participant(self, channel_id: UUID, participant: Participant) -> None: ...

    @abstractmethod
    async def remove_participant(self, channel_id: UUID, user_id: UUID) -> None: ...

    @abstractmethod
    async def get_participants(self, channel_id: UUID) -> list[Participant]: ...


# i_channel_verifier.py
class IChannelVerifier(ABC):
    @abstractmethod
    async def verify_channel_exists(self, channel_id: UUID) -> bool: ...
```

### `JoinChannelHandler`

```python
class JoinChannelHandler:
    def __init__(
        self,
        sfu: ISfuService,
        session_repo: ISessionRepo,
        channel_verifier: IChannelVerifier,
        event_publisher: IEventPublisher,
    ):
        self._sfu = sfu
        self._session_repo = session_repo
        self._channel_verifier = channel_verifier
        self._events = event_publisher

    async def handle(self, cmd: JoinChannelCommand) -> JoinChannelResult:
        # 1. Verify channel exists (gRPC → community-svc)
        if not await self._channel_verifier.verify_channel_exists(cmd.channel_id):
            raise ChannelNotFoundError(cmd.channel_id)

        # 2. Get or create voice session
        session = await self._session_repo.find_session(cmd.channel_id)
        if session is None:
            session = VoiceSession.create(cmd.channel_id)

        # 3. Add participant (enforces 25-person limit)
        participant = session.add_participant(cmd.user_id)

        # 4. Create SFU WebRtcTransport
        transport_params = await self._sfu.create_transport(
            channel_id=str(cmd.channel_id),
            user_id=str(cmd.user_id)
        )

        # 5. Persist session state to Redis
        await self._session_repo.save_session(session)
        await self._session_repo.add_participant(cmd.channel_id, participant)

        # 6. Publish domain events
        for event in session.pull_events():
            await self._events.publish(event)

        return JoinChannelResult(
            transport_id=transport_params["transport_id"],
            ice_parameters=transport_params["ice_parameters"],
            ice_candidates=transport_params["ice_candidates"],
            dtls_parameters=transport_params["dtls_parameters"],
            rtp_capabilities=transport_params["router_rtp_capabilities"],
        )
```

---

## Infrastructure layer

### `AiortcSfuAdapter` (`infrastructure/sfu/aiortc_sfu_adapter.py`)

```python
from aiortc import RTCPeerConnection, RTCSessionDescription
from aiortc.contrib.media import MediaRelay

class AiortcSfuAdapter(ISfuService):
    def __init__(self):
        self._routers: dict[str, MediaRelay] = {}     # channel_id → MediaRelay
        self._transports: dict[str, RTCPeerConnection] = {}  # transport_id → PC

    async def get_or_create_router(self, channel_id: str) -> str:
        if channel_id not in self._routers:
            self._routers[channel_id] = MediaRelay()
            logger.info(f"Created SFU router for channel {channel_id}")
        return channel_id

    async def create_transport(self, channel_id: str, user_id: str) -> dict:
        pc = RTCPeerConnection()
        transport_id = str(uuid4())
        self._transports[transport_id] = pc

        @pc.on("icecandidate")
        async def on_ice_candidate(candidate):
            pass  # candidates sent back via WS signaling

        return {
            "transport_id": transport_id,
            "ice_parameters": {"usernameFragment": pc.localDescription},
            "ice_candidates": [],
            "dtls_parameters": {},
            "router_rtp_capabilities": self._get_rtp_capabilities(),
        }

    async def close_transport(self, transport_id: str) -> None:
        if transport_id in self._transports:
            await self._transports[transport_id].close()
            del self._transports[transport_id]

    async def destroy_router(self, channel_id: str) -> None:
        if channel_id in self._routers:
            del self._routers[channel_id]
            logger.info(f"Destroyed SFU router for channel {channel_id}")
```

### `RedisSessionRepo` (`infrastructure/persistence/redis_session_repo.py`)

```python
import json
import redis.asyncio as aioredis

class RedisSessionRepo(ISessionRepo):
    KEY_PREFIX = "voice"
    PARTICIPANT_TTL = 65   # seconds — refreshed by heartbeat every 30s

    def __init__(self, redis: aioredis.Redis):
        self._redis = redis

    async def add_participant(self, channel_id: UUID, participant: Participant) -> None:
        key = f"{self.KEY_PREFIX}:{channel_id}:participants"
        value = json.dumps({
            "user_id": str(participant.user_id),
            "joined_at": participant.joined_at.isoformat(),
            "status": participant.status.value,
        })
        await self._redis.hset(key, str(participant.user_id), value)
        await self._redis.expire(key, self.PARTICIPANT_TTL)

    async def remove_participant(self, channel_id: UUID, user_id: UUID) -> None:
        key = f"{self.KEY_PREFIX}:{channel_id}:participants"
        await self._redis.hdel(key, str(user_id))

    async def get_participants(self, channel_id: UUID) -> list[Participant]:
        key = f"{self.KEY_PREFIX}:{channel_id}:participants"
        raw = await self._redis.hgetall(key)
        return [self._deserialise(v) for v in raw.values()]
```

### `KafkaVoicePublisher` (`infrastructure/messaging/kafka_voice_publisher.py`)

```python
from aiokafka import AIOKafkaProducer
import json

class KafkaVoicePublisher(IEventPublisher):
    TOPIC_MAP = {
        "VoiceSessionStarted": "voice.session.started",
        "VoiceSessionEnded":   "voice.session.ended",
    }

    async def publish(self, event: object) -> None:
        topic = self.TOPIC_MAP.get(type(event).__name__)
        if not topic:
            return
        payload = json.dumps(event.__dict__, default=str).encode()
        await self._producer.send_and_wait(topic, payload)
```

### `ChannelGrpcClient` (`infrastructure/grpc/channel_grpc_client.py`)

```python
import grpc
from .protos import channel_service_pb2, channel_service_pb2_grpc

class ChannelGrpcClient(IChannelVerifier):
    def __init__(self, target: str):
        channel = grpc.aio.insecure_channel(target)
        self._stub = channel_service_pb2_grpc.ChannelServiceStub(channel)

    async def verify_channel_exists(self, channel_id: UUID) -> bool:
        try:
            req = channel_service_pb2.VerifyChannelExistsRequest(
                channel_id=str(channel_id)
            )
            resp = await self._stub.VerifyChannelExists(req, timeout=0.5)
            return resp.exists
        except grpc.aio.AioRpcError:
            return False
```

---

## API layer

### WebSocket signaling handler (`api/websocket/signaling_handler.py`)

```python
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends

ws_router = APIRouter()

@ws_router.websocket("/ws/voice/{channel_id}")
async def voice_websocket(
    channel_id: str,
    websocket: WebSocket,
    user_id: str = Depends(get_current_user_ws),   # extract from JWT query param
    handler: SignalingHandler = Depends(get_signaling_handler),
):
    await websocket.accept()
    try:
        async for raw_msg in websocket.iter_text():
            msg = SignalingMessage.model_validate_json(raw_msg)
            response = await handler.handle(msg, channel_id, user_id)
            if response:
                await websocket.send_text(response.model_dump_json())
    except WebSocketDisconnect:
        await handler.on_disconnect(channel_id, user_id)
```

### WebSocket message types

```python
from pydantic import BaseModel
from enum import Enum

class MessageType(str, Enum):
    JOIN          = "join"
    LEAVE         = "leave"
    SDP_OFFER     = "sdp_offer"
    SDP_ANSWER    = "sdp_answer"
    ICE_CANDIDATE = "ice_candidate"
    MUTE          = "mute"
    PARTICIPANT_JOINED = "participant_joined"
    PARTICIPANT_LEFT   = "participant_left"
    SERVER_CAPABILITIES = "server_capabilities"

class SignalingMessage(BaseModel):
    type: MessageType
    data: dict = {}

class JoinMessage(BaseModel):
    channel_id: str
    rtp_capabilities: dict

class SdpOfferMessage(BaseModel):
    transport_id: str
    sdp: str

class IceCandidateMessage(BaseModel):
    transport_id: str
    candidate: dict
```

### REST endpoints

| Method | Path | Description | Auth |
|---|---|---|---|
| `GET` | `/api/v1/channels/:id/participants` | List active participants in a voice channel | ✅ JWT |
| `GET` | `/api/v1/sessions/:channelId` | Get session metadata | ✅ JWT |
| `GET` | `/health` | Health check (Redis + Kafka status) | ❌ |
| `GET` | `/metrics` | Prometheus metrics | ❌ internal |

---

## WebRTC flows

### 1. Join channel

```
Client ──WS──► voice-svc
  send: { type: "join", data: { channel_id, rtp_capabilities } }
  recv: { type: "server_capabilities", data: { transport_id, ice_params, dtls_params, rtp_caps } }
```

### 2. SDP negotiation

```
Client                              SFU
  createOffer()
  send: { type: "sdp_offer", data: { transport_id, sdp } }
                              ──► setRemoteDescription(offer)
                              ──► createAnswer()
  recv: { type: "sdp_answer", data: { sdp } }
  setRemoteDescription(answer)

  (ICE candidates trickled bidirectionally via WS)
  send: { type: "ice_candidate", data: { transport_id, candidate } }
  recv: { type: "ice_candidate", data: { candidate } }

  DTLS handshake completes ──► SRTP keys derived ──► media can flow
```

### 3. Produce audio/video

```
Client ──► send: { type: "produce", data: { transport_id, kind, rtp_params } }
SFU   ──► creates Producer, returns producer_id
SFU   ──► broadcasts participant_joined to all other participants in channel
Other clients ──► create_consumer for new producer_id ──► SRTP media flows
```

### 4. Leave / disconnect

```
Client ──► WS disconnect (or send: { type: "leave" })
SFU:
  close WebRtcTransport for that user
  remove all their Producers and Consumers
  notify remaining participants: { type: "participant_left", user_id }
  if session empty → destroy Router
  publish Kafka: voice.session.ended
```

---

## SFU internals

The `aiortc` SFU uses a **MediaRelay** to forward media tracks between participants:

```
Client A mic ──SRTP──► Producer (A's audio)
                           │
                     MediaRelay (per channel)
                           │
              ┌────────────┼────────────┐
         Consumer(B)  Consumer(C)  Consumer(N)
              │              │           │
         Client B         Client C    Client N
         decodes          decodes     decodes
```

**Key property:** the SFU forwards raw SRTP packets. It never decodes audio or video. This means:
- CPU cost per sender = O(1) regardless of participant count
- Bandwidth cost scales as O(n) where n = number of participants
- Privacy: audio data is never decrypted on the server

### Audio codec

Harmony uses **Opus** (48kHz, 2 channels, bitrate adaptive 16–128kbps). Video uses **VP8** or **H.264** depending on client capability negotiation.

### Noise suppression

Implemented client-side via the browser's built-in `mediaDevices.getUserMedia` noise suppression constraints. The server never processes audio content.

---

## Kafka events

### Published by voice-svc

| Event | Topic | Payload | Consumers |
|---|---|---|---|
| `VoiceSessionStarted` | `voice.session.started` | `{ session_id, channel_id, started_at }` | `ws-gateway`, `notification-svc` |
| `VoiceSessionEnded` | `voice.session.ended` | `{ session_id, channel_id, duration_seconds, ended_at }` | `ws-gateway` |
| `ParticipantJoined` | `voice.participant.joined` | `{ channel_id, user_id, joined_at }` | `ws-gateway` (show indicator) |
| `ParticipantLeft` | `voice.participant.left` | `{ channel_id, user_id, left_at }` | `ws-gateway` |

### Consumed by voice-svc

None. Voice-svc does not consume Kafka events — it is triggered only by WebSocket connections.

---

## Database and state

### Redis (primary state store)

| Key pattern | Value | TTL | Purpose |
|---|---|---|---|
| `voice:{channelId}:participants` | Hash: userId → JSON | 65s (refreshed by heartbeat) | Active participants |
| `voice:{channelId}:session` | JSON VoiceSession | 65s | Session metadata |
| `voice:{channelId}:transport:{userId}` | JSON transport params | 65s | SFU transport state per user |

Clients must send a heartbeat every 30 seconds. If the heartbeat stops, the TTL expires and the participant is automatically considered disconnected.

### PostgreSQL (session history only)

```sql
CREATE TABLE voice_sessions (
    id           UUID PRIMARY KEY,
    channel_id   UUID NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    ended_at     TIMESTAMPTZ,
    peak_participants INT NOT NULL DEFAULT 0,
    duration_seconds  INT
);

CREATE TABLE voice_participants (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id   UUID NOT NULL REFERENCES voice_sessions(id),
    user_id      UUID NOT NULL,
    joined_at    TIMESTAMPTZ NOT NULL,
    left_at      TIMESTAMPTZ,
    duration_seconds INT
);
CREATE INDEX idx_voice_participants_session ON voice_participants (session_id);
```

---

## Local development — localhost setup

### Prerequisites

```bash
python --version        # 3.12+
pip --version           # 23+
docker --version        # 24+
docker-compose --version

# Recommended: use uv for fast dependency management
pip install uv
```

### Step 1 — Clone and set up the environment

```bash
git clone https://github.com/harmony/voice-svc.git
cd voice-svc

# Create virtual environment
python -m venv .venv
source .venv/bin/activate        # Linux / macOS
# .venv\Scripts\activate         # Windows

# Install dependencies
pip install -r requirements-dev.txt

# Or with uv (much faster):
uv pip install -r requirements-dev.txt
```

### Step 2 — Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```env
# ── Database ───────────────────────────────────────────────────────────────
DATABASE_URL=postgresql+asyncpg://harmony:harmony@localhost:5432/harmony_voice

# ── Redis ──────────────────────────────────────────────────────────────────
REDIS_URL=redis://localhost:6379/0

# ── Kafka ──────────────────────────────────────────────────────────────────
KAFKA_BROKERS=localhost:9092
KAFKA_GROUP_ID=voice-svc

# ── Auth — JWKS endpoint (from auth-svc)
JWKS_URL=http://localhost:8084/api/v1/auth/.well-known/jwks.json
JWT_AUDIENCE=harmony-api
JWT_ISSUER=harmony-auth

# ── gRPC: community-svc (channel verification)
COMMUNITY_SVC_GRPC=localhost:9082

# ── SFU / WebRTC ────────────────────────────────────────────────────────────
# IP announced in ICE candidates — use your LAN IP for local multi-device testing
ANNOUNCED_IP=127.0.0.1
# UDP port range for media
RTC_MIN_PORT=40000
RTC_MAX_PORT=49999

# ── HTTP ────────────────────────────────────────────────────────────────────
HTTP_PORT=8086

# ── App ─────────────────────────────────────────────────────────────────────
APP_ENV=development
LOG_LEVEL=debug
```

> **ANNOUNCED_IP:** For local testing in a browser on the same machine, `127.0.0.1` works. For testing from another device on the same LAN, set this to your machine's LAN IP (e.g. `192.168.1.50`). For production, set it to the public IP or use a TURN server.

### Step 3 — Start infrastructure

```bash
docker-compose up -d
```

The `docker-compose.yml` starts:

| Container | Port | Purpose |
|---|---|---|
| `postgres` | `5432` | PostgreSQL 15 |
| `redis` | `6379` | Redis 7 |
| `kafka` | `9092` | Apache Kafka |
| `zookeeper` | `2181` | Kafka coordinator |
| `kafka-ui` | `8090` | Browse Kafka topics (optional) |

Verify containers:

```bash
docker-compose ps
# All should show "healthy" after ~20 seconds
```

### Step 4 — Run database migrations

```bash
# Using psql directly
docker exec -it $(docker ps -qf "name=postgres") \
  psql -U harmony -d harmony_voice \
  -f /dev/stdin < migrations/001_create_voice_history.sql

# Or with Alembic (if configured):
alembic upgrade head
```

### Step 5 — Start the service

```bash
# Development mode (auto-reload on file changes)
uvicorn src.main:app --reload --port 8086 --host 0.0.0.0

# Or with the Makefile shortcut:
make dev
```

Expected output:

```
INFO:     Started server process [12345]
INFO:     Waiting for application startup.
INFO:     [voice-svc] Connected to Redis at redis://localhost:6379/0
INFO:     [voice-svc] Connected to PostgreSQL
INFO:     [voice-svc] Kafka producer ready — brokers: localhost:9092
INFO:     [voice-svc] SFU adapter initialised — ports 40000-49999
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8086 (Press CTRL+C to quit)
```

### Step 6 — Verify the service

```bash
# Health check
curl http://localhost:8086/health
# → {"status":"ok","redis":"connected","postgres":"connected","kafka":"connected"}

# List participants in a channel (should return empty list)
curl http://localhost:8086/api/v1/channels/<channelId>/participants \
  -H "Authorization: Bearer <jwt>"
```

### Step 7 — Test WebSocket signaling

Install `websocat` for quick WS testing:

```bash
brew install websocat   # macOS
# or: cargo install websocat

# Connect to a voice channel (replace <channelId> and <token>)
websocat "ws://localhost:8086/ws/voice/<channelId>?token=<jwt>"

# Send join message
{"type":"join","data":{"rtp_capabilities":{"codecs":[{"mimeType":"audio/opus","clockRate":48000,"channels":2}],"headerExtensions":[]}}}

# Expected response:
# {"type":"server_capabilities","data":{"transport_id":"...","ice_parameters":{...},...}}
```

### Step 8 — Run tests

```bash
# Unit tests (fast, no infrastructure)
pytest tests/unit/ -v

# Integration tests (requires Docker containers running)
pytest tests/integration/ -v

# All tests with coverage
pytest --cov=src --cov-report=html
open htmlcov/index.html

# Run only fast tests
pytest -m "not slow" -v

# Run with asyncio debug mode
PYTHONASYNCIODEBUG=1 pytest tests/ -v
```

### Useful `make` targets

```makefile
make dev          # start uvicorn with --reload
make test         # run all tests
make test-unit    # unit tests only
make test-int     # integration tests
make lint         # ruff + mypy
make format       # ruff format
make migrate      # run alembic upgrade head
make docker-up    # docker-compose up -d
make docker-down  # docker-compose down
```

### Common issues

| Problem | Cause | Fix |
|---|---|---|
| `OSError: [Errno 98] Address already in use` | Port 8086 taken | `lsof -i :8086` then kill the process |
| `ConnectionRefusedError: Redis` | Redis not started | `docker-compose up -d redis` |
| `ICE failed — no candidates` | ANNOUNCED_IP wrong | Set `ANNOUNCED_IP` to your machine's actual LAN IP |
| `RTCPeerConnection failed` | Port range blocked | Allow UDP `40000-49999` inbound in firewall |
| `grpc._channel._InactiveRpcError` | community-svc not running | Start community-svc or set `SKIP_CHANNEL_VERIFY=true` in `.env` for local dev |
| `ModuleNotFoundError: aiortc` | venv not activated | `source .venv/bin/activate` |
| Kafka consumer lag | Service restarting | Normal on start — wait 5s for Kafka to be ready |

### Docker run (alternative to local Python)

```bash
# Build the image
docker build -t harmony-voice-svc .

# Run with env vars
docker run -p 8086:8086 -p 40000-49999:40000-49999/udp \
  --env-file .env \
  harmony-voice-svc
```

> **Note:** the UDP port range (`40000-49999`) must be exposed for WebRTC media. Do not skip this in Docker.

### Stopping everything

```bash
# Stop uvicorn: Ctrl+C

# Stop Docker containers
docker-compose down

# Full cleanup (removes volumes and DB data)
docker-compose down -v
```

---

## Testing strategy

| Layer | Type | Tool | Dependencies |
|---|---|---|---|
| `domain/` | Unit | `pytest` | None — pure Python dataclasses |
| `application/` | Unit | `pytest` + `unittest.mock` | Mocked ports via `AsyncMock` |
| `infrastructure/sfu/` | Integration | `pytest-asyncio` | aiortc in-process |
| `infrastructure/persistence/` | Integration | `pytest-asyncio` + `testcontainers` | Real Redis container |
| `api/websocket/` | Integration | `httpx` + `websockets` | Full app via `TestClient` |

### Example domain unit test

```python
import pytest
from uuid import uuid4
from src.domain.voice_session.voice_session import VoiceSession
from src.domain.voice_session.voice_session_events import VoiceSessionStarted

def test_create_voice_session_raises_started_event():
    channel_id = uuid4()
    session = VoiceSession.create(channel_id)
    events = session.pull_events()
    assert len(events) == 1
    assert isinstance(events[0], VoiceSessionStarted)
    assert events[0].channel_id == channel_id

def test_add_participant_raises_channel_full_after_25():
    session = VoiceSession.create(uuid4())
    for _ in range(25):
        session.add_participant(uuid4())
    with pytest.raises(ChannelFullError):
        session.add_participant(uuid4())

def test_remove_last_participant_raises_session_ended():
    session = VoiceSession.create(uuid4())
    user_id = uuid4()
    session.add_participant(user_id)
    session.pull_events()   # clear started event
    session.remove_participant(user_id)
    events = session.pull_events()
    assert any(isinstance(e, VoiceSessionEnded) for e in events)
```

### Example application unit test

```python
import pytest
from unittest.mock import AsyncMock
from src.application.commands.join_channel.join_channel_handler import JoinChannelHandler
from src.application.commands.join_channel.join_channel_command import JoinChannelCommand

@pytest.mark.asyncio
async def test_join_raises_not_found_when_channel_missing():
    verifier = AsyncMock()
    verifier.verify_channel_exists.return_value = False

    handler = JoinChannelHandler(
        sfu=AsyncMock(),
        session_repo=AsyncMock(),
        channel_verifier=verifier,
        event_publisher=AsyncMock(),
    )

    with pytest.raises(ChannelNotFoundError):
        await handler.handle(JoinChannelCommand(
            channel_id=uuid4(),
            user_id=uuid4(),
        ))
```

---

## Environment variables

| Variable | Default | Required | Description |
|---|---|---|---|
| `DATABASE_URL` | — | ✅ | PostgreSQL async DSN (`postgresql+asyncpg://...`) |
| `REDIS_URL` | `redis://localhost:6379/0` | ✅ | Redis connection URL |
| `KAFKA_BROKERS` | `localhost:9092` | ✅ | Comma-separated Kafka brokers |
| `KAFKA_GROUP_ID` | `voice-svc` | — | Kafka consumer group |
| `HTTP_PORT` | `8086` | — | HTTP / WebSocket port |
| `JWKS_URL` | — | ✅ | auth-svc JWKS endpoint for JWT verification |
| `JWT_AUDIENCE` | `harmony-api` | — | Expected JWT audience claim |
| `JWT_ISSUER` | `harmony-auth` | — | Expected JWT issuer claim |
| `COMMUNITY_SVC_GRPC` | `localhost:9082` | ✅ | community-svc gRPC address |
| `ANNOUNCED_IP` | `127.0.0.1` | ✅ | IP included in WebRTC ICE candidates |
| `RTC_MIN_PORT` | `40000` | — | UDP port range start for media |
| `RTC_MAX_PORT` | `49999` | — | UDP port range end for media |
| `PARTICIPANT_TTL_S` | `65` | — | Redis TTL for participant records |
| `MAX_PARTICIPANTS` | `25` | — | Max participants per voice channel |
| `SKIP_CHANNEL_VERIFY` | `false` | — | Skip gRPC channel check (local dev only) |
| `LOG_LEVEL` | `info` | — | `debug` / `info` / `warn` / `error` |
| `APP_ENV` | `development` | — | `development` / `staging` / `production` |

---

## Related services

| Service | Relationship |
|---|---|
| `auth-svc` | JWT verification via JWKS endpoint; never called per-message — public key cached |
| `community-svc` | Called via gRPC (`VerifyChannelExists`) before allowing a participant to join |
| `ws-gateway` | Consumes `voice.session.started` / `voice.participant.joined` to show speaking indicators in the channel list |
| `notification-svc` | Consumes `voice.session.started` to push "X joined voice" notification to friends |
| `user-svc` | Called via gRPC to resolve display name for participant list responses |

---

*Part of the Harmony platform monorepo — see the root [README](../../README.md) for the full architecture overview.*