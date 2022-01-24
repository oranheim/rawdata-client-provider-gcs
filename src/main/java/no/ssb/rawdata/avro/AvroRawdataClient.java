package no.ssb.rawdata.avro;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataCursor;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNoSuchPositionException;
import no.ssb.rawdata.api.RawdataProducer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AvroRawdataClient implements RawdataClient {

    static final Logger LOG = LoggerFactory.getLogger(AvroRawdataClient.class);

    final AtomicBoolean closed = new AtomicBoolean(false);

    final Path tmpFileFolder;
    final long avroMaxSeconds;
    final long avroMaxBytes;
    final int avroSyncInterval;
    final int fileListingMinIntervalSeconds;

    final List<AvroRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<AvroRawdataConsumer> consumers = new CopyOnWriteArrayList<>();
    final AvroRawdataUtils readOnlyAvroRawdataUtils;
    final AvroRawdataUtils readWriteAvroRawdataUtils;

    public AvroRawdataClient(Path tmpFileFolder, long avroMaxSeconds, long avroMaxBytes, int avroSyncInterval, int fileListingMinIntervalSeconds, AvroRawdataUtils readOnlyAvroRawdataUtils, AvroRawdataUtils readWriteAvroRawdataUtils) {
        this.tmpFileFolder = tmpFileFolder;
        this.avroMaxSeconds = avroMaxSeconds;
        this.avroMaxBytes = avroMaxBytes;
        this.avroSyncInterval = avroSyncInterval;
        this.fileListingMinIntervalSeconds = fileListingMinIntervalSeconds;
        this.readOnlyAvroRawdataUtils = readOnlyAvroRawdataUtils;
        this.readWriteAvroRawdataUtils = readWriteAvroRawdataUtils;
    }

    @Override
    public RawdataProducer producer(String topic) {
        if (closed.get()) {
            throw new RawdataClosedException();
        }
        AvroRawdataProducer producer = new AvroRawdataProducer(readWriteAvroRawdataUtils, tmpFileFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, topic);
        producers.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, RawdataCursor cursor) {
        if (closed.get()) {
            throw new RawdataClosedException();
        }
        AvroRawdataConsumer consumer = new AvroRawdataConsumer(readOnlyAvroRawdataUtils, topic, (AvroRawdataCursor) cursor, fileListingMinIntervalSeconds);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public RawdataCursor cursorOf(String topic, ULID.Value ulid, boolean inclusive) {
        return new AvroRawdataCursor(ulid, inclusive);
    }

    @Override
    public RawdataCursor cursorOf(String topic, String position, boolean inclusive, long approxTimestamp, Duration tolerance) throws RawdataNoSuchPositionException {
        return cursorOf(topic, ulidOfPosition(topic, position, approxTimestamp, tolerance), inclusive);
    }

    private ULID.Value ulidOfPosition(String topic, String position, long approxTimestamp, Duration tolerance) throws RawdataNoSuchPositionException {
        ULID.Value lowerBoundUlid = RawdataConsumer.beginningOf(approxTimestamp - tolerance.toMillis());
        ULID.Value upperBoundUlid = RawdataConsumer.beginningOf(approxTimestamp + tolerance.toMillis());
        try (AvroRawdataConsumer consumer = new AvroRawdataConsumer(readOnlyAvroRawdataUtils, topic, new AvroRawdataCursor(lowerBoundUlid, true), fileListingMinIntervalSeconds)) {
            RawdataMessage message;
            while ((message = consumer.receive(0, TimeUnit.SECONDS)) != null) {
                if (message.timestamp() > upperBoundUlid.timestamp()) {
                    throw new RawdataNoSuchPositionException(
                            String.format("Unable to find position, reached upper-bound. Time-range=[%s,%s), position=%s",
                                    formatTimestamp(lowerBoundUlid.timestamp()),
                                    formatTimestamp(upperBoundUlid.timestamp()),
                                    position));
                }
                if (position.equals(message.position())) {
                    return message.ulid(); // found matching position
                }
            }
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new RawdataNoSuchPositionException(
                String.format("Unable to find position, reached end-of-stream. Time-range=[%s,%s), position=%s",
                        formatTimestamp(lowerBoundUlid.timestamp()),
                        formatTimestamp(upperBoundUlid.timestamp()),
                        position));
    }

    String formatTimestamp(long timestamp) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("YYYY-MM-dd'T'HH:mm:ss.SSS");
        LocalDateTime dt = LocalDateTime.ofInstant(new Date(timestamp).toInstant(), ZoneOffset.UTC);
        return dt.format(dtf);
    }

    @Override
    public RawdataMessage lastMessage(String topic) throws RawdataClosedException {
        NavigableMap<Long, RawdataAvroFile> topicBlobs = readOnlyAvroRawdataUtils.getTopicBlobs(topic);
        if (topicBlobs.isEmpty()) {
            return null;
        }
        RawdataAvroFile rawdataAvroFile = topicBlobs.lastEntry().getValue();
        LOG.debug("Reading last message from RawdataAvroFile: {}", rawdataAvroFile);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(AvroRawdataProducer.schema);
        DataFileReader<GenericRecord> dataFileReader;
        try {
            dataFileReader = new DataFileReader<>(rawdataAvroFile.seekableInput(), datumReader);
            dataFileReader.seek(rawdataAvroFile.getOffsetOfLastBlock());
            GenericRecord record = null;
            while (dataFileReader.hasNext()) {
                record = dataFileReader.next(record);
            }
            return record == null ? null : AvroRawdataConsumer.toRawdataMessage(record);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (AvroRawdataProducer producer : producers) {
                producer.close();
            }
            producers.clear();
            for (AvroRawdataConsumer consumer : consumers) {
                consumer.close();
            }
            consumers.clear();
        }
    }
}
