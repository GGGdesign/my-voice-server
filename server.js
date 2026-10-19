
"use strict";

const express = require("express");
const http = require("http");
const fs = require("fs");
const admin = require("firebase-admin");
const { Server } = require("socket.io");

const app = express();
const httpServer = http.createServer(app);

const io = new Server(httpServer, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  maxHttpBufferSize: 10 * 1024 * 1024
});

app.use(express.json({ limit: "16kb" }));

const PORT = Number(process.env.PORT || 10000);
const DB_URL =
  "https://maxvoice-a5e31-default-rtdb.europe-west1.firebasedatabase.app";
const SECRET_PATH =
  "/etc/secrets/firebase-service-account.json";

const GIFTS_ENABLED =
  process.env.MAXCHAT_ENABLE_GIFTS !== "false";

const GIFTS = Object.freeze({
  rose: { name: "وردة ملكية", price: 100 },
  heart: { name: "قلب الحب", price: 200 },
  bear: { name: "دبدوب", price: 500 },
  crown: { name: "تاج ملكي", price: 1000 },
  bouquet: { name: "باقة ورد", price: 2000 },
  car: { name: "سيارة فاخرة", price: 5000 },
  yacht: { name: "يخت", price: 10000 },
  palace: { name: "قصر", price: 20000 },
  diamond: { name: "ألماس", price: 50000 },
  horse: { name: "حصان ملكي", price: 100000 },
  dragon: { name: "تنين أسطوري", price: 500000 },
  galaxy: { name: "المجرة", price: 1000000 },
  tree: { name: "شجرة الذهب", price: 2000000 },
  throne: { name: "عرش ملكي", price: 5000000 },
  kingdom: { name: "مملكة الذهب", price: 10000000 }
});

let firebaseReady = false;

try {
  const credentials = JSON.parse(
    fs.readFileSync(SECRET_PATH, "utf8")
  );

  admin.initializeApp({
    credential: admin.credential.cert(credentials),
    databaseURL: DB_URL
  });

  firebaseReady = true;
  console.log("FIREBASE READY");
} catch (error) {
  console.error("FIREBASE ERROR:", error.message);
}

function db() {
  if (!firebaseReady) {
    throw new Error("FIREBASE_NOT_READY");
  }
  return admin.database();
}

const userRef = uid => db().ref("Users").child(uid);

function validId(value) {
  return typeof value === "string" &&
    /^[a-zA-Z0-9_-]{1,100}$/.test(value);
}

function validRequestId(value) {
  return typeof value === "string" &&
    /^[a-zA-Z0-9_-]{16,100}$/.test(value);
}

function validCoins(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function errorResult(error) {
  return { ok: false, error };
}

async function transactionReady(ref, updater) {
  let listener = null;

  try {
    await new Promise((resolve, reject) => {
      listener = snapshot => resolve(snapshot);
      ref.on("value", listener, reject);
    });

    return await ref.transaction(
      updater,
      undefined,
      false
    );
  } finally {
    if (listener) {
      ref.off("value", listener);
    }
  }
}

async function verifyToken(token) {
  if (!firebaseReady) {
    throw new Error("SERVER_NOT_READY");
  }

  if (!token || typeof token !== "string") {
    throw new Error("TOKEN_REQUIRED");
  }

  try {
    return await admin.auth().verifyIdToken(token, true);
  } catch {
    throw new Error("INVALID_TOKEN");
  }
}

async function verifyHttpUser(req) {
  const header = String(
    req.headers.authorization || ""
  );

  const match = /^Bearer\s+(.+)$/i.exec(header);

  if (!match) {
    throw new Error("TOKEN_REQUIRED");
  }

  return verifyToken(match[1]);
}

async function readWallet(uid, operation) {
  if (!validId(uid)) {
    return errorResult("INVALID_UID");
  }

  const snapshot = await userRef(uid).once("value");

  if (!snapshot.exists()) {
    console.error("USER NOT FOUND:", {
      operation,
      uid
    });
    return errorResult("USER_NOT_FOUND");
  }

  const user = snapshot.val();

  if (!user || typeof user !== "object") {
    return errorResult("INVALID_USER");
  }

  const coins = Number(user.coins);

  if (!validCoins(coins)) {
    return errorResult("INVALID_WALLET");
  }

  return { ok: true, uid, coins, user };
}

function profileName(user) {
  return String(
    user?.name ||
    user?.userName ||
    user?.username ||
    user?.nickname ||
    "مستخدم"
  );
}

// =====================================
// GIFTS
// =====================================

function sameGiftPayment(payment, request) {
  return Boolean(
    payment &&
    payment.roomId === request.roomId &&
    payment.giftId === request.giftId &&
    payment.receiverId === request.receiverId &&
    payment.quantity === request.quantity &&
    payment.amount === request.amount
  );
}

async function giftReceiverOnSeat(roomId, receiverId) {
  const snapshot = await db()
    .ref("rooms")
    .child(roomId)
    .child("seats")
    .once("value");

  let found = false;

  snapshot.forEach(seat => {
    const userId = String(
      seat.child("userId").val() || ""
    );

    const userUid = String(
      seat.child("userUid").val() || ""
    );

    if (
      userId === receiverId ||
      userUid === receiverId
    ) {
      found = true;
    }
  });

  return found;
}

async function chargeGift(uid, request) {
  const wallet = await readWallet(uid, "GIFT_SEND");

  if (!wallet.ok) return wallet;

  let reason = "PAYMENT_REJECTED";

  const tx = await transactionReady(
    userRef(uid),
    user => {
      if (!user) {
        reason = "USER_NOT_FOUND";
        return;
      }

      const payments = user.giftPayments || {};
      const previous = payments[request.requestId];

      if (previous) {
        reason = sameGiftPayment(previous, request)
          ? "ALREADY_PAID"
          : "REQUEST_ID_CONFLICT";
        return;
      }

      const coins = Number(user.coins);

      if (!validCoins(coins)) {
        reason = "INVALID_WALLET";
        return;
      }

      if (coins < request.amount) {
        reason = "INSUFFICIENT_COINS";
        return;
      }

      return {
        ...user,
        coins: coins - request.amount,
        giftPayments: {
          ...payments,
          [request.requestId]: {
            roomId: request.roomId,
            giftId: request.giftId,
            receiverId: request.receiverId,
            quantity: request.quantity,
            amount: request.amount,
            status: "paid",
            createdAt: Date.now()
          }
        }
      };
    }
  );

  const saved = tx.snapshot.val();
  const payment =
    saved?.giftPayments?.[request.requestId];

  if (!sameGiftPayment(payment, request)) {
    return errorResult(reason);
  }

  return {
    ok: true,
    newlyCharged: tx.committed,
    balance: Number(saved.coins),
    senderId: String(saved.customId || ""),
    senderName: profileName(saved),
    createdAt: payment.createdAt
  };
}

async function publishGift(uid, request, payment) {
  const eventId = `${uid}_${request.requestId}`;

  const event = {
    requestId: request.requestId,
    senderUid: uid,
    senderId: payment.senderId,
    senderName: payment.senderName,
    receiverId: request.receiverId,
    giftId: request.giftId,
    quantity: request.quantity,
    amount: request.amount,
    createdAt: payment.createdAt
  };

  const ref = db()
    .ref("rooms")
    .child(request.roomId)
    .child("giftEvents")
    .child(eventId);

  await transactionReady(ref, current => {
    if (current !== null) return;
    return event;
  });

  const saved = (
    await ref.once("value")
  ).val();

  if (
    !saved ||
    saved.senderUid !== uid ||
    saved.requestId !== request.requestId ||
    saved.giftId !== request.giftId ||
    saved.receiverId !== request.receiverId ||
    saved.quantity !== request.quantity ||
    saved.amount !== request.amount
  ) {
    throw new Error("GIFT_EVENT_CONFLICT");
  }

  return saved;
}

function giftError(res, status, error) {
  return res.status(status).json({
    ok: false,
    error
  });
}

app.get("/api/gifts/catalog", (req, res) => {
  res.json({
    ok: true,
    enabled: GIFTS_ENABLED && firebaseReady,
    gifts: GIFTS
  });
});

app.post("/api/gifts/send", async (req, res) => {
  if (!firebaseReady) {
    return giftError(res, 503, "SERVER_NOT_READY");
  }

  if (!GIFTS_ENABLED) {
    return giftError(res, 503, "GIFTS_DISABLED");
  }

  let decoded;

  try {
    decoded = await verifyHttpUser(req);
  } catch (error) {
    return giftError(res, 401, error.message);
  }

  const uid = decoded.uid;
  const roomId = String(req.body?.roomId || "");
  const giftId = String(req.body?.giftId || "");
  const receiverId = String(req.body?.receiverId || "");
  const requestId = String(req.body?.requestId || "");
  const quantity = req.body?.quantity;

  if (
    !validId(roomId) ||
    !validId(giftId) ||
    !validId(receiverId) ||
    !validRequestId(requestId) ||
    !Number.isSafeInteger(quantity) ||
    quantity < 1 ||
    quantity > 99
  ) {
    return giftError(
      res,
      400,
      "INVALID_GIFT_REQUEST"
    );
  }

  const gift = GIFTS[giftId];

  if (!gift) {
    return giftError(res, 400, "UNKNOWN_GIFT");
  }

  const amount = gift.price * quantity;

  if (!validCoins(amount) || amount === 0) {
    return giftError(res, 400, "INVALID_AMOUNT");
  }

  const request = {
    roomId,
    giftId,
    receiverId,
    requestId,
    quantity,
    amount
  };

  try {
    const wallet = await readWallet(
      uid,
      "GIFT_REQUEST"
    );

    if (!wallet.ok) {
      return giftError(res, 409, wallet.error);
    }

    const previous =
      wallet.user.giftPayments?.[requestId];

    if (previous) {
      if (!sameGiftPayment(previous, request)) {
        return giftError(
          res,
          409,
          "REQUEST_ID_CONFLICT"
        );
      }
    } else {
      const exists = await giftReceiverOnSeat(
        roomId,
        receiverId
      );

      if (!exists) {
        return giftError(
          res,
          404,
          "RECEIVER_NOT_ON_SEAT"
        );
      }
    }

    const payment = await chargeGift(
      uid,
      request
    );

    if (!payment.ok) {
      return giftError(
        res,
        payment.error === "INSUFFICIENT_COINS"
          ? 402
          : 409,
        payment.error
      );
    }

    await publishGift(uid, request, payment);

    return res.json({
      ok: true,
      requestId,
      roomId,
      giftId,
      receiverId,
      quantity,
      amount,
      balance: payment.balance,
      alreadyPaid: !payment.newlyCharged
    });
  } catch (error) {
    console.error(
      "GIFT SEND ERROR:",
      error.message
    );

    return res.status(503).json({
      ok: false,
      error: "GIFT_STATUS_UNCERTAIN",
      requestId
    });
  }
});

// =====================================
// VOICE + ROOMS
// =====================================

io.on("connection", socket => {
  console.log("USER CONNECTED:", socket.id);

  socket.on("join_room", data => {
    if (!data?.roomId) return;

    socket.join(String(data.roomId));

    socket.to(String(data.roomId)).emit(
      "user_joined_room",
      {
        userId: data.userId,
        userName: data.userName
      }
    );
  });

  socket.on("toggle_mute", data => {
    if (!data?.roomId) return;

    io.to(String(data.roomId)).emit(
      "user_toggle_mute",
      {
        userId: data.userId,
        isMuted: data.isMuted
      }
    );
  });

  socket.on("sit_on_seat", data => {
    if (!data?.roomId) return;

    io.to(String(data.roomId)).emit(
      "user_sat_on_seat",
      {
        userId: data.userId,
        seatIndex: data.seatIndex
      }
    );
  });

  socket.on("leave_seat", data => {
    if (!data?.roomId) return;

    io.to(String(data.roomId)).emit(
      "user_left_seat",
      {
        userId: data.userId
      }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(String(data.roomId)).emit(
      "user_started_audio",
      {
        userId: data.userId
      }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(String(data.roomId)).emit(
      "user_stopped_audio",
      {
        userId: data.userId
      }
    );
  });

  socket.on("voice_data", data => {
    if (!data?.roomId) return;

    socket.to(String(data.roomId)).emit(
      "voice_data",
      {
        audioBuffer: data.audioBuffer,
        userId: data.userId
      }
    );
  });

  socket.on("disconnect", () => {
    console.log("USER DISCONNECTED:", socket.id);
  });
});

// =====================================
// HEALTH
// =====================================

app.get("/", (req, res) => {
  res.json({
    app: "Max Chat",
    status: "online",
    service: "voice_rooms_and_gifts",
    firebaseReady,
    giftsEnabled:
      firebaseReady && GIFTS_ENABLED
  });
});

// =====================================
// START
// =====================================

httpServer.listen(PORT, () => {
  console.log("MAX CHAT SERVER RUNNING:", PORT);
  console.log("FIREBASE READY:", firebaseReady);
  console.log("GIFTS ENABLED:", GIFTS_ENABLED);
});
