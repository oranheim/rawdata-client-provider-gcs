import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.avro.cloudstorage.GCSRawdataClientInitializer;
import no.ssb.rawdata.avro.filesystem.FilesystemAvroRawdataClientInitializer;

module no.ssb.rawdata.avro {
    requires no.ssb.rawdata.api;
    requires no.ssb.service.provider.api;
    requires org.slf4j;
    requires org.apache.avro;

    requires gax;
    requires google.cloud.storage;
    requires google.cloud.core;
    requires com.google.auth.oauth2;
    requires com.google.auth;
    requires com.google.gson;

    provides RawdataClientInitializer with GCSRawdataClientInitializer, FilesystemAvroRawdataClientInitializer;
}