package org.example;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

public class DisruptorService {

    static Logger logger = LoggerFactory.getLogger(DisruptorService.class);

    public static class TranslationReqEvent {
        private String key;

        public void setKey(String key) {
            this.key = key;
        }
    }

    public static class TranslationReqEventFactory implements EventFactory<TranslationReqEvent> {
        @Override
        public TranslationReqEvent newInstance() {
            return new TranslationReqEvent();
        }
    }

    public static class TranslationReqEventHandler implements EventHandler<TranslationReqEvent> {
        RingBuffer<TranslationRespEvent> respRingBuffer;
        ByteBuffer bb = ByteBuffer.allocate(32);
        ByteBuffer bb2 = ByteBuffer.allocate(32);

        public TranslationReqEventHandler(RingBuffer<TranslationRespEvent> respRingBuffer) {
            this.respRingBuffer = respRingBuffer;
        }

        @Override
        public void onEvent(TranslationReqEvent event, long sequence, boolean endOfBatch) {
            logger.info("TranslationReqEvent: " + event.key);

            byte[] key = event.key.getBytes(StandardCharsets.UTF_8);
            byte[] replace = event.key.replace("key", "replace").getBytes(StandardCharsets.UTF_8);
            bb.put(key, 0, key.length);
            bb2.put(replace, 0, replace.length);
            respRingBuffer.publishEvent((e, s, buffer, buffer2) -> {
                e.setKey(new String(buffer.array(), 0, key.length, StandardCharsets.UTF_8));
                e.setTranslation(new String(buffer2.array(), 0, replace.length, StandardCharsets.UTF_8));
            }, bb, bb2);
            bb.clear();
            bb2.clear();
        }
    }

    public static class TranslationRespEvent {
        private String key;
        private String translation;

        public void setKey(String key) {
            this.key = key;
        }

        public void setTranslation(String translation) {
            this.translation = translation;
        }
    }

    public static class TranslationRespEventFactory implements EventFactory<TranslationRespEvent> {
        @Override
        public TranslationRespEvent newInstance() {
            return new TranslationRespEvent();
        }
    }

    public static class TranslationRespEventHandler implements EventHandler<TranslationRespEvent> {

        ConcurrentHashMap<String, String> stateMap;

        public TranslationRespEventHandler(ConcurrentHashMap<String, String> stateMap) {
            this.stateMap = stateMap;
        }

        @Override
        public void onEvent(TranslationRespEvent event, long sequence, boolean endOfBatch) {
            logger.info("TranslationRespEvent: " + event.key + " " + event.translation);
            stateMap.put(event.key, event.translation);
        }
    }


    public static void main(String[] args) {
        int bufferSize = 32 * 128 * 1024;

        ConcurrentHashMap<String, String> stateMap = new ConcurrentHashMap<>();

        Disruptor<TranslationRespEvent> respEventDisruptor =
                new Disruptor<>(new TranslationRespEventFactory(), bufferSize, DaemonThreadFactory.INSTANCE);

        TranslationRespEventHandler translationRespEventHandler = new TranslationRespEventHandler(stateMap);
        respEventDisruptor.handleEventsWith(translationRespEventHandler);
        respEventDisruptor.start();

        RingBuffer<TranslationRespEvent> respRingBuffer = respEventDisruptor.getRingBuffer();

        Disruptor<TranslationReqEvent> reqEventDisruptor =
                new Disruptor<>(new TranslationReqEventFactory(), bufferSize, DaemonThreadFactory.INSTANCE);

        TranslationReqEventHandler translationReqEventHandler = new TranslationReqEventHandler(respRingBuffer);
        reqEventDisruptor.handleEventsWith(translationReqEventHandler);
        reqEventDisruptor.start();


        RingBuffer<TranslationReqEvent> ringBuffer = reqEventDisruptor.getRingBuffer();
        ByteBuffer bb = ByteBuffer.allocate(32);
        for (int i = 0; i < 100_000; i++) {
            int j = i % 100;
            byte[] in = ("key" + j).getBytes(StandardCharsets.UTF_8);
            bb.put(in, 0, in.length);
            ringBuffer.publishEvent((event, sequence, buffer) -> event.setKey(new String(buffer.array(), 0, in.length, StandardCharsets.UTF_8)), bb);
            bb.clear();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        stateMap.entrySet().stream().sorted().forEach(e -> logger.info("{} to {}", e.getKey(), e.getValue()));
    }

}
