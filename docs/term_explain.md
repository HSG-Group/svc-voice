# SDP stands for Session Description Protocol.

It is a text-based format used in telecommunications, streaming, and WebRTC to negotiate media capabilities between two endpoints (like your web browser and a server or another user) before starting a media session.

SDP doesn't deliver audio or video data itself; instead, it acts as a declaration of features and parameters so both sides can agree on how to communicate.

### What Information SDP Contains
When two peers perform an SDP exchange (Offer/Answer), the SDP message carries details such as:

- Media Types: What streams are being sent (e.g., audio, video, data channels).

- Codecs: Which audio/video codecs are supported (e.g., VP8, H.264, Opus) and their exact parameters (sample rate, channels).

- IP Addresses & Ports: Transport details showing where to send the incoming media streams.

- Bandwidth & Control Info: Maximum expected bitrates and stream attributes (e.g., sendrecv, sendonly, recvonly).

- Security Credentials: Keys and fingerprints used for establishing encrypted connections (such as DTLS/SRTP).

### How SDP Works in WebRTC
In WebRTC, SDP is used during the Offer / Answer negotiation process:

1. Offer: The initiator generates a local SDP description containing its capabilities and sends it to the remote party via a 2.signaling channel.

3. Answer: The receiving party processes the offer, matches it against its own capabilities, generates an SDP answer, and sends it back.

3. Session Establishment: Once both parties set their local and remote SDP descriptions (setLocalDescription / setRemoteDescription), they establish transport channels (via ICE/DTLS) and begin sending encrypted media streams (SRTP).
