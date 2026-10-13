const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

io.on('connection', (socket) => {
    console.log('مستخدم متصل بالسيرفر');

    socket.on('join_room', (data) => {
        const { roomId, userId, userName } = data;
        socket.join(roomId);
        socket.to(roomId).emit('user_joined_room', { userId, userName });
    });

    socket.on('toggle_mute', (data) => {
        const { roomId, userId, isMuted } = data;
        io.to(roomId).emit('user_toggle_mute', { userId, isMuted });
    });

    socket.on('sit_on_seat', (data) => {
        const { roomId, userId, seatIndex } = data;
        io.to(roomId).emit('user_sat_on_seat', { userId, seatIndex });
    });

    socket.on('leave_seat', (data) => {
        const { roomId, userId } = data;
        io.to(roomId).emit('user_left_seat', { userId });
    });

    socket.on('start_audio_stream', (data) => {
        const { roomId, userId } = data;
        socket.to(roomId).emit('user_started_audio', { userId });
    });

    socket.on('stop_audio_stream', (data) => {
        const { roomId, userId } = data;
        socket.to(roomId).emit('user_stopped_audio', { userId });
    });

    socket.on('voice_data', (data) => {
        const { roomId, audioBuffer, userId } = data;
        socket.to(roomId).emit('voice_data', { audioBuffer, userId });
    });

    socket.on('disconnect', () => {
        console.log('مستخدم غادر السيرفر');
    });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log(`السيرفر الصوتي يعمل بنجاح على المنفذ ${PORT}`);
});
