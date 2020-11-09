package edu.ucr.cs.cs226.ydong044;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.URI;
import java.net.URL;


/**
 * Hello world!
 *
 */
public class HDFSUpload
{
    public static void main( String[] args )
    {
        // /Users/michael/Downloads/AREAWATER.csv
        // hdfs:///out/AREAWATER.csv
        // /Users/michael/Downloads/out/AREAWATER.csv

        String srcPathName, dstPathName;
        if(args.length==2){
            srcPathName = args[0];
            dstPathName = args[1];
        } else {
            System.err.println("Require two arguments, srcPathName and dstPathName");
            return;
        }

        System.out.format("-> Copying FROM \"%s\" TO \"%s\"\n", srcPathName, dstPathName);
        long start = System.currentTimeMillis();
        try {
            FileService fileService = new FileService();
            fileService.copy(srcPathName, dstPathName);
            System.out.format("EXECUTION TIME: %dms\n", System.currentTimeMillis()-start);
//            fileService.delete(dstPathName);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}

class FileService {
    private static final String NAME_NODE = "hdfs://localhost:9000";
    private final FileSystem lFS;
    private final FileSystem dFS;
    private final Configuration configuration;

    public FileService() throws Exception {
        configuration = new Configuration();
        /***
         * hadoop common and hadoop-hdfs are all declaring DistributedFileSystemClass, thus we
         * need to clarify that here
         */
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory(configuration));
        lFS = FileSystem.getLocal(configuration);
        dFS = FileSystem.get(new URI(NAME_NODE), configuration);
    };

    public void copy(String srcPathName, String dstPathName) throws Exception {
        FileSystem srcFS = getFS(srcPathName);
        FileSystem dstFS = getFS(dstPathName);
        Path srcPath = getPath(srcPathName);
        Path dstPath = getPath(dstPathName);

        DataInputStream inputStream = srcFS.open(srcPath);
        DataOutputStream outputStream = dstFS.create(dstPath, false);

        IOUtils.copyBytes(inputStream, outputStream, 4096, true);
    }

    private FileSystem getFS(String pathName) {
        try {
            var url = new URL(pathName);
            if(url.getProtocol().equals("hdfs")){
                System.out.println("using dFS");
                return dFS;
            } else {
                return lFS;
            }
        } catch(Exception e){
            return lFS;
        }
    }

    private Path getPath(String pathName) throws Exception {
        var uri = new URI(pathName);
        return new Path(uri.getPath());
    }

    public void delete(String pathName) throws Exception {
        FileSystem fs = getFS(pathName);
        Path path = getPath(pathName);
        fs.delete(path, true);
    }
}
