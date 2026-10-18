
/* ==========================================
   MAX CHAT - SECURE GIFTS API
   POST /api/gifts/send
   ========================================== */

const GIFT_PRICES = Object.freeze({
  rose: 100,
  heart: 200,
  flower: 500,
  cake: 1000,
  car: 5000,
  crown: 10000
});

app.post("/api/gifts/send", async (req, res) => {
  if (!firebaseReady) {
    return res.status(503).json({
      ok: false,
      error: "SERVER_NOT_READY"
    });
  }

  let decoded;

  try {
    decoded = await verifyRequestUser(req);
  } catch (error) {
    return res.status(error.status || 401).json({
      ok: false,
      error: error.message
    });
  }

  const uid = decoded.uid;
  const roomId = String(req.body?.roomId || "");
  const giftId = String(req.body?.giftId || "");
  const receiverId = String(req.body?.receiverId || "");
  const requestId = String(req.body?.requestId || "");
  const quantity = Number(req.body?.quantity || 1);

  if (
    !/^[a-zA-Z0-9_-]{1,100}$/.test(roomId) ||
    !/^[a-zA-Z0-9_-]{1,50}$/.test(giftId) ||
    !/^[0-9]{1,20}$/.test(receiverId) ||
    !/^[a-zA-Z0-9_-]{16,100}$/.test(requestId) ||
    !Number.isSafeInteger(quantity) ||
    quantity < 1 ||
    quantity > 99 ||
    !Object.prototype.hasOwnProperty.call(
      GIFT_PRICES,
      giftId
    )
  ) {
    return res.status(400).json({
      ok: false,
      error: "INVALID_GIFT_REQUEST"
    });
  }

  const amount = GIFT_PRICES[giftId] * quantity;

  if (!Number.isSafeInteger(amount)) {
    return res.status(400).json({
      ok: false,
      error: "INVALID_AMOUNT"
    });
  }

  try {
    const roomReference = db()
      .ref("rooms")
      .child(roomId);

    const seatsSnapshot = await roomReference
      .child("seats")
      .once("value");

    let receiverFound = false;

    seatsSnapshot.forEach(seat => {
      if (
        String(seat.child("userId").val() || "") ===
        receiverId
      ) {
        receiverFound = true;
      }
    });

    if (!receiverFound) {
      return res.status(404).json({
        ok: false,
        error: "RECEIVER_NOT_ON_SEAT"
      });
    }

    let failure = "PAYMENT_REJECTED";
    let alreadyPaid = false;

    const walletResult = await userRef(uid)
      .transaction(user => {
        if (!user) {
          failure = "USER_NOT_FOUND";
          return;
        }

        const payments = user.giftPayments || {};
        const existing = payments[requestId];

        if (existing) {
          if (
            existing.roomId === roomId &&
            existing.giftId === giftId &&
            existing.receiverId === receiverId &&
            existing.quantity === quantity &&
            existing.amount === amount
          ) {
            alreadyPaid = true;
            failure = "ALREADY_PAID";
          } else {
            failure = "REQUEST_ID_CONFLICT";
          }

          return;
        }

        const coins = Number(user.coins);

        if (
          !Number.isSafeInteger(coins) ||
          coins < 0 ||
          coins < amount
        ) {
          failure = "INSUFFICIENT_COINS";
          return;
        }

        return {
          ...user,
          coins: coins - amount,
          giftPayments: {
            ...payments,
            [requestId]: {
              roomId,
              giftId,
              receiverId,
              quantity,
              amount,
              status: "paid",
              createdAt: Date.now()
            }
          }
        };
      }, undefined, false);

    const savedUser = walletResult.snapshot.val();
    const payment = savedUser
      ?.giftPayments
      ?.[requestId];

    if (!walletResult.committed && !alreadyPaid) {
      return res.status(409).json({
        ok: false,
        error: failure
      });
    }

    if (
      !payment ||
      payment.roomId !== roomId ||
      payment.giftId !== giftId ||
      payment.receiverId !== receiverId ||
      payment.quantity !== quantity ||
      payment.amount !== amount
    ) {
      return res.status(409).json({
        ok: false,
        error: "PAYMENT_NOT_CONFIRMED"
      });
    }

    const giftEvent = {
      requestId,
      senderUid: uid,
      senderId: String(savedUser.customId || ""),
      senderName: String(savedUser.name || "مستخدم"),
      receiverId,
      giftId,
      quantity,
      amount,
      createdAt: payment.createdAt
    };

    const eventResult = await roomReference
      .child("giftEvents")
      .child(requestId)
      .transaction(current => {
        if (current !== null) return;
        return giftEvent;
      }, undefined, false);

    const event = eventResult.snapshot.val();

    if (
      !event ||
      event.senderUid !== uid ||
      event.giftId !== giftId ||
      event.receiverId !== receiverId
    ) {
      return res.status(409).json({
        ok: false,
        error: "GIFT_EVENT_CONFLICT"
      });
    }

    return res.json({
      ok: true,
      requestId,
      giftId,
      quantity,
      amount,
      receiverId,
      balance: savedUser.coins,
      alreadyPaid: !walletResult.committed
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

/* END SECURE GIFTS API */
