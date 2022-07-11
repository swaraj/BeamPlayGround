package org.example;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

public class ReplacerService {
    static Disruptor<DisruptorService.TranslationRespEvent> respEventDisruptor;
    static Disruptor<DisruptorService.TranslationReqEvent> reqEventDisruptor;

    static ConcurrentHashMap<String, String> stateMap = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, String> cacheMap = new ConcurrentHashMap<>();

    static RingBuffer<DisruptorService.TranslationReqEvent> ringBuffer;

    public static void start() {
        int bufferSize = 32 * 128 * 1024;
        respEventDisruptor =
                new Disruptor<>(new DisruptorService.TranslationRespEventFactory(), bufferSize, DaemonThreadFactory.INSTANCE);

        DisruptorService.TranslationRespEventHandler translationRespEventHandler = new DisruptorService.TranslationRespEventHandler(stateMap);
        respEventDisruptor.handleEventsWith(translationRespEventHandler);
        respEventDisruptor.start();

        RingBuffer<DisruptorService.TranslationRespEvent> respRingBuffer = respEventDisruptor.getRingBuffer();

        reqEventDisruptor =
                new Disruptor<>(new DisruptorService.TranslationReqEventFactory(), bufferSize, DaemonThreadFactory.INSTANCE);

        DisruptorService.TranslationReqEventHandler translationReqEventHandler = new DisruptorService.TranslationReqEventHandler(respRingBuffer);
        reqEventDisruptor.handleEventsWith(translationReqEventHandler);
        reqEventDisruptor.start();

        ringBuffer = reqEventDisruptor.getRingBuffer();
    }

    public static void shutdown() {
        reqEventDisruptor.shutdown();
        respEventDisruptor.shutdown();
    }

    public static String getReplacement(String key) {
        return cacheMap.computeIfAbsent(key, k -> {
            ByteBuffer bb = ByteBuffer.allocate(32);

            byte[] in = key.getBytes(StandardCharsets.UTF_8);
            bb.put(in, 0, in.length);

            ringBuffer.publishEvent((event, sequence, buffer) -> event.setKey(new String(buffer.array(), 0, in.length, StandardCharsets.UTF_8)), bb);

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return stateMap.get(key);
        });
    }
}
