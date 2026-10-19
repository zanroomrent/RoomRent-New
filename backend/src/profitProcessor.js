"use strict";

const admin = require("firebase-admin");

const db = admin.firestore();
const DAY_MS = 24 * 60 * 60 * 1000;
const MAX_CYCLE_DAYS = 90;

/**
 * Chakata booking moja.
 *
 * Masharti:
 * - Admin amethibitisha malipo.
 * - Booking iko active.
 * - Saa 24 kamili zimepita tangu profitStartAt.
 * - Kila siku inalipwa mara moja tu.
 *
 * Tahadhari:
 * Hii ni ledger ya salio la Firestore, si uthibitisho
 * kwamba fedha halisi zinapatikana kwa ajili ya malipo.
 */
async function processBooking(bookingDoc, nowMs) {
    const bookingRef = bookingDoc.ref;
    const initial = bookingDoc.data();

    if (
        initial.adminConfirmed !== true ||
        initial.paymentStatus !== "verified" ||
        initial.bookingStatus !== "active" ||
        initial.profitStatus !== "running"
    ) {
        return { skipped: true, reason: "BOOKING_NOT_ELIGIBLE" };
    }

    const uid = String(initial.userId || "").trim();
    const dailyProfit = Number(initial.dailyProfit);

    if (!uid || !Number.isSafeInteger(dailyProfit) || dailyProfit <= 0) {
        console.error("Invalid booking data:", bookingDoc.id);
        return { skipped: true, reason: "INVALID_BOOKING_DATA" };
    }

    const startTimestamp = initial.profitStartAt;

    if (!startTimestamp || typeof startTimestamp.toMillis !== "function") {
        console.error("Missing profitStartAt:", bookingDoc.id);
        return { skipped: true, reason: "MISSING_START_TIME" };
    }

    const startMs = startTimestamp.toMillis();

    if (nowMs < startMs + DAY_MS) {
        return { skipped: true, reason: "FIRST_24_HOURS_NOT_COMPLETE" };
    }

    const elapsedFullDays = Math.floor((nowMs - startMs) / DAY_MS);
    const cycleDays = Math.min(
        MAX_CYCLE_DAYS,
        Math.max(1, Number(initial.durationDays) || MAX_CYCLE_DAYS)
    );

    const eligibleDays = Math.min(elapsedFullDays, cycleDays);

    if (eligibleDays < 1) {
        return { skipped: true, reason: "NO_PROFIT_DUE" };
    }

    const userRef = db.collection("users").doc(uid);

    // Kumbukumbu tofauti kwa kila booking na kila siku.
    // Transaction huzuia kuingiza faida mara mbili.
    let creditedDays = 0;

    await db.runTransaction(async (tx) => {
        const freshBookingDoc = await tx.get(bookingRef);

        if (!freshBookingDoc.exists) return;

        const booking = freshBookingDoc.data();

        if (
            booking.adminConfirmed !== true ||
            booking.paymentStatus !== "verified" ||
            booking.bookingStatus !== "active" ||
            booking.profitStatus !== "running"
        ) {
            return;
        }

        const freshStart = booking.profitStartAt;

        if (!freshStart || typeof freshStart.toMillis !== "function") {
            return;
        }

        const freshStartMs = freshStart.toMillis();

        const freshElapsedDays = Math.floor(
            (nowMs - freshStartMs) / DAY_MS
        );

        const freshCycleDays = Math.min(
            MAX_CYCLE_DAYS,
            Math.max(1, Number(booking.durationDays) || MAX_CYCLE_DAYS)
        );

        const daysDue = Math.min(freshElapsedDays, freshCycleDays);

        if (daysDue < 1) return;

        const ledgerRef = db.collection("profitLedger");

        // Read all documents before making any transaction writes.
        const ledgerQuery = ledgerRef
            .where("bookingId", "==", bookingDoc.id);

        const ledgerSnapshot = await tx.get(ledgerQuery);

        const alreadyCredited = new Set(
            ledgerSnapshot.docs.map((doc) => Number(doc.data().dayNumber))
        );

        const userDoc = await tx.get(userRef);

        if (!userDoc.exists) {
            throw new Error("USER_NOT_FOUND");
        }

        const user = userDoc.data();
        const balance = Number(user.balance);

        if (!Number.isSafeInteger(balance) || balance < 0) {
            throw new Error("INVALID_USER_BALANCE");
        }

        let amountToCredit = 0;
        const newLedgerEntries = [];

        for (let day = 1; day <= daysDue; day++) {
            if (alreadyCredited.has(day)) continue;

            const ledgerDocId = `${bookingDoc.id}_day_${day}`;
            const entryRef = ledgerRef.doc(ledgerDocId);

            amountToCredit += dailyProfit;

            newLedgerEntries.push({
                ref: entryRef,
                data: {
                    bookingId: bookingDoc.id,
                    userId: uid,
                    dayNumber: day,
                    amount: dailyProfit,
                    createdAt: admin.firestore.FieldValue.serverTimestamp()
                }
            });
        }

        if (newLedgerEntries.length === 0) {
            if (daysDue >= freshCycleDays) {
                tx.update(bookingRef, {
                    profitStatus: "completed",
                    profitCompletedAt:
                        admin.firestore.FieldValue.serverTimestamp(),
                    updatedAt:
                        admin.firestore.FieldValue.serverTimestamp()
                });
            }
            return;
        }

        const newBalance = balance + amountToCredit;

        if (!Number.isSafeInteger(newBalance)) {
            throw new Error("BALANCE_OVERFLOW");
        }

        // Salio na ledger vinaandikwa ndani ya transaction moja.
        tx.update(userRef, {
            balance: newBalance,
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
        });

        for (const entry of newLedgerEntries) {
            tx.create(entry.ref, entry.data);
        }

        const totalCreditedDays = alreadyCredited.size + newLedgerEntries.length;

        tx.update(bookingRef, {
            totalProfitCredited: admin.firestore.FieldValue.increment(amountToCredit),
            lastProfitProcessedAt:
                admin.firestore.FieldValue.serverTimestamp(),
            profitStatus:
                totalCreditedDays >= freshCycleDays ? "completed" : "running",
            ...(totalCreditedDays >= freshCycleDays
                ? {
                    profitCompletedAt:
                        admin.firestore.FieldValue.serverTimestamp()
                }
                : {}),
            updatedAt: admin.firestore.FieldValue.serverTimestamp()
        });

        creditedDays = newLedgerEntries.length;
    });

    return {
        bookingId: bookingDoc.id,
        creditedDays,
        status: "processed"
    };
}

/**
 * Kazi kuu ya kuendesha processor.
 * Inaweza kuitwa na Railway Cron au scheduled worker.
 */
async function runProfitProcessor() {
    const nowMs = Date.now();

    const snapshot = await db.collection("bookings")
        .where("adminConfirmed", "==", true)
        .where("paymentStatus", "==", "verified")
        .where("bookingStatus", "==", "active")
        .where("profitStatus", "==", "running")
        .get();

    const results = [];

    for (const bookingDoc of snapshot.docs) {
        try {
            results.push(await processBooking(bookingDoc, nowMs));
        } catch (error) {
            console.error(
                `Profit processing failed for ${bookingDoc.id}:`,
                error.message
            );

            results.push({
                bookingId: bookingDoc.id,
                error: error.message
            });
        }
    }

    return {
        checked: snapshot.size,
        results
    };
}

module.exports = { runProfitProcessor };
