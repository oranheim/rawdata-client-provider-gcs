package no.ssb.rawdata.avro.filesystem;

import no.ssb.rawdata.api.RawdataMetadataClient;
import no.ssb.rawdata.avro.AvroRawdataClient;
import no.ssb.rawdata.avro.AvroRawdataUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FilesystemRawdataClient extends AvroRawdataClient {

    final Path storageFolder;

    public FilesystemRawdataClient(Path tmpFileFolder, long avroMaxSeconds, long avroMaxBytes, int avroSyncInterval, int fileListingMinIntervalSeconds, AvroRawdataUtils readOnlyAvroRawdataUtils, AvroRawdataUtils readWriteAvroRawdataUtils, Path storageFolder) {
        super(tmpFileFolder, avroMaxSeconds, avroMaxBytes, avroSyncInterval, fileListingMinIntervalSeconds, readOnlyAvroRawdataUtils, readWriteAvroRawdataUtils);
        this.storageFolder = storageFolder;
    }

    @Override
    public RawdataMetadataClient metadata(String topic) {
        Path metadataFolder = createMetadataFolderIfNotExists(topic);
        return new FilesystemRawdataMetadataClient(metadataFolder, topic);
    }

    Path createMetadataFolderIfNotExists(String topic) {
        Path metadataFolder = storageFolder.resolve(topic).resolve("metadata");
        try {
            Files.createDirectories(metadataFolder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return metadataFolder;
    }
}
