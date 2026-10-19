
"use strict";

const express = require("express");
const http = require("http");
const fs = require("fs");
const admin = require("firebase-admin");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  maxHttpBufferSize: 10 * 1024 * 1024
});

app.use(express.json({ limit: "16kb" }));

const PORT = Number(process.env.PORT || 10000);

const DB_URL =
  "https://maxvoice-a5e31-default-rtdb.europe-west1.firebasedatabase.app";

const SECRET_PATH =
  process.env.FIREBASE_SECRET_PATH ||
  "/etc/secrets/firebase-service-account.json";

const GIFTS_ENABLED =
  process.env.MAXCHAT_ENABLE_GIFTS !== "false";

const GIFTS = Object.freeze({
  rose: {
    name: "وردة ملكية",
    price: 100
  },
  heart: {
    name: "قلب الحب",
    price: 200
  },
  bear: {
    name: "دبدوب",
    price: 500
  },
  crown: {
    name: "تاج ملكي",
    price: 1000
  },
  bouquet: {
    name: "باقة ورد",
    price: 2000
  },
  car: {
    name: "سيارة فاخرة",
    price: 5000
  },
  yacht: {
    name: "يخت",
    price: 10000
  },
  palace: {
    name: "قصر",
    price: 20000
  },
  diamond: {
    name: "ألماس",
    price: 50000
  },
  horse: {
    name: "حصان ملكي",
    price: 100000
  },
  dragon: {
    name: "تنين أسطوري",
    price: 500000
  },
  galaxy: {
    name: "المجرة",
    price: 1000000
  },
  tree: {
    name: "شجرة الذهب",
    price: 2000000
  },
  throne: {
    name: "عرش ملكي",
    price: 5000000
  },
  kingdom: {
    name: "مملكة الذهب",
    price: 10000000
  }
});

let ready = false;

try {
  const credentials = JSON.parse(
    process.env.FIREBASE_SERVICE_ACCOUNT_JSON ||
    fs.readFileSync(SECRET_PATH, "utf8")
  );

  admin.initializeApp({
    credential: admin.credential.cert(credentials),
    databaseURL: DB_URL
  });

  ready = true;

  console.log("VOICE FIREBASE READY");
} catch (e) {
  console.error(
    "VOICE FIREBASE ERROR",
    e.message
  );
}

const db = () => admin.database();

const refUser = uid =>
  db().ref("Users").child(uid);

const validId = v =>
  typeof v === "string" &&
  /^[a-zA-Z0-9_-]{1,100}$/.test(v);

const validReq = v =>
  typeof v === "string" &&
  /^[a-zA-Z0-9_-]{16,100}$/.test(v);

const money = n =>
  Number.isSafeInteger(n) && n >= 0;

const tx = (ref, fn) =>
  ref.transaction(fn, undefined, false);

const nameOf = u =>
  String(
    u?.name ||
    u?.userName ||
    u?.username ||
    u?.nickname ||
    "مستخدم"
  );

const fail = (res, status, error) =>
  res.status(status).json({
    ok: false,
    error
  });

async function auth(req) {
  const match = /^Bearer\s+(.+)$/i.exec(
    String(req.headers.authorization || "")
  );

  if (!match) {
    throw new Error("TOKEN_REQUIRED");
  }

  try {
    return await admin.auth()
      .verifyIdToken(match[1], true);
  } catch {
    throw new Error("INVALID_TOKEN");
  }
}

function same(p, r) {
  return !!p &&
    p.roomId === r.roomId &&
    p.giftId === r.giftId &&
    p.receiverId === r.receiverId &&
    p.quantity === r.quantity &&
    p.amount === r.amount;
}

async function recipientPresent(
  roomId,
  receiverId
) {
  const snap = await db()
    .ref("rooms")
    .child(roomId)
    .child("seats")
    .once("value");

  let found = false;

  snap.forEach(s => {
    const userId = String(
      s.child("userId").val() || ""
    );

    const userUid = String(
      s.child("userUid").val() || ""
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

async function payGift(uid, r) {
  let reason = "PAYMENT_REJECTED";

  const result = await tx(
    refUser(uid),
    u => {
      if (!u) {
        reason = "USER_NOT_FOUND";
        return;
      }

      const payments =
        u.giftPayments || {};

      if (payments[r.requestId]) {
        reason = same(
          payments[r.requestId],
          r
        )
          ? "ALREADY_PAID"
          : "REQUEST_ID_CONFLICT";

        return;
      }

      const balance = Number(u.coins);

      if (!money(balance)) {
        reason = "INVALID_WALLET";
        return;
      }

      if (balance < r.amount) {
        reason = "INSUFFICIENT_COINS";
        return;
      }

      return {
        ...u,
        coins: balance - r.amount,
        giftPayments: {
          ...payments,
          [r.requestId]: {
            roomId: r.roomId,
            giftId: r.giftId,
            receiverId: r.receiverId,
            quantity: r.quantity,
            amount: r.amount,
            status: "paid",
            createdAt: Date.now()
          }
        }
      };
    }
  );

  const u = result.snapshot.val();

  const payment =
    u?.giftPayments?.[r.requestId];

  if (!same(payment, r)) {
    return {
      ok: false,
      error: reason
    };
  }

  return {
    ok: true,
    newlyCharged: result.committed,
    balance: Number(u.coins),
    senderId: String(u.customId || ""),
    senderName: nameOf(u),
    createdAt: payment.createdAt
  };
}

async function showGift(uid, r, paid) {
  const eventId =
    `${uid}_${r.requestId}`;

  const event = {
    requestId: r.requestId,
    senderUid: uid,
    senderId: paid.senderId,
    senderName: paid.senderName,
    receiverId: r.receiverId,
    giftId: r.giftId,
    quantity: r.quantity,
    amount: r.amount,
    createdAt: paid.createdAt
  };

  const ref = db()
    .ref("rooms")
   
