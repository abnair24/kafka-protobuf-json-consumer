package com.github.abnair24.util;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 *
 */
public class ProtoUtility {


    private static final Logger logger = LoggerFactory.getLogger(ProtoUtility.class);


    public static Path getDescriptorBinary(ProtoDetail protoDetail) throws IOException, InterruptedException {

        Path descFilePath = Files.createTempFile("ProtoDesc", ".desc");
        logger.info("Descriptor Binary path:",descFilePath.toAbsolutePath().toString());

        ImmutableList<String> protocArgs = ImmutableList.<String>builder()
                .add("--include_imports")
                .add("--include_std_types")
                .add("--proto_path=" + protoDetail.getProtoPath())
                .add("--descriptor_set_out=" + descFilePath.toAbsolutePath().toString())
                .addAll(protoDetail.getProtoFiles())
                .build();


        int status = new ProtocInvoker().invoke(protocArgs);

        if (status != 0) {
            logger.error("Binary file generation failed with status : " + status);
        }
        return descFilePath;
    }
}
