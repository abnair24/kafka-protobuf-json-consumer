package com.github.abnair24.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The type Proto cache.
 */
public class ProtoCache {

    private static final Logger logger = LoggerFactory.getLogger(ProtoCache.class);

    private static LoadingCache<DescriptorProtos.FileDescriptorProto, Descriptors.FileDescriptor> fdCache = CacheBuilder
            .newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(30, TimeUnit.MINUTES)
            .recordStats()
            .build(new CacheLoader<DescriptorProtos.FileDescriptorProto, Descriptors.FileDescriptor>() {
                @Override
                public Descriptors.FileDescriptor load(DescriptorProtos.FileDescriptorProto key) throws Exception {
                    return ProtoBufDecoder.getAllFileDescriptors(key);
                }
            });


    private static Cache<String, Path> descriptorBinaryCache = CacheBuilder
            .newBuilder()
            .maximumSize(100)
            .expireAfterWrite(30,TimeUnit.MINUTES)
            .recordStats()
            .build();


    public static Descriptors.FileDescriptor getFileDescriptor(DescriptorProtos.FileDescriptorProto fileDescriptorProto) throws Exception {
        Descriptors.FileDescriptor fileDescriptor = fdCache.get(fileDescriptorProto);
        logger.info("Stats:{}", fdCache.stats());

        return fileDescriptor;
    }

    public static Set<DescriptorProtos.FileDescriptorProto> getAllFileDescriptorFromCache() {
        return fdCache.asMap().keySet();
    }

    public static Path getBinary(ProtoDetail protoDetail) throws Exception {

        Path path = descriptorBinaryCache.getIfPresent("descriptor.desc");
        logger.info("DescriptorCache Stats: {}",descriptorBinaryCache.stats());
        if (path == null) {
            path = ProtoUtility.getDescriptorBinary(protoDetail);
            descriptorBinaryCache.put("descriptor.desc", path);
        }
        return path;
    }
}
