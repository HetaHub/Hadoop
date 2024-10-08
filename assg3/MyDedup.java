import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.lang.*;
import java.security.MessageDigest;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

class FingerprintIndex {
    private String checksum;
    private int offset;
    private int size;
    private String containerFileName;
    private int count = 0;

    public FingerprintIndex() {
    }

    public String getChecksum() {
        return this.checksum;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public int getOffset() {
        return this.offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public String getContainerFileName() {
        return this.containerFileName;
    }

    public void setContainerFileName(String fn) {
        this.containerFileName = fn;
    }

    public int getSize() {
        return this.size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getCount() {
        return this.count;
    }

    public void addCount() {
        this.count += 1;
    }

    public void reduceCount() {
        this.count -= 1;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public boolean isCountZero() {
        return this.count == 0;
    }

    @Override
    public String toString() {
        return (this.checksum).toString() + "-" + this.offset + "-" + this.containerFileName + "-" + this.count + "-" + this.size + "\n";
    }
}

class FileRecipe {
    private String filename;
    private List<FingerprintIndex> fpi_list = new ArrayList<FingerprintIndex>();

    public void addFpi(FingerprintIndex fpi) {
        this.fpi_list.add(fpi);
    }

    public List<FingerprintIndex> getFpiList() {
        return this.fpi_list;
    }

    public String getFileName() {
        return filename;
    }

    public void setFileName(String x) {
        this.filename = x;
    }

}

class Chunk {
    private String checksum;
    private byte[] data;
    private int size;
    private int count; // number of files using this chunk

    public String getChecksum() {
        return this.checksum;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public byte[] getData() {
        return this.data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public int getSize() {
        return this.size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getCount() {
        return this.count;
    }

    public void addCount() {
        this.count += 1;
    }

    public void reduceCount() {
        this.count -= 1;
    }

    public Boolean checkCount() {
        return this.count == 0; // return true if the chunk is not used
    }
}

class Container {
    // private int id;
    private List<Chunk> chunks = new ArrayList<Chunk>();
    private int remainingSize = 1048576;
    private String fileName = "";

    public Container() {
        this.chunks = new ArrayList<Chunk>();
        this.remainingSize = 1048576;
        this.fileName = "";
    }

    public void initialize() {
        this.chunks = new ArrayList<Chunk>();
        this.remainingSize = 1048576;
        this.fileName = "";
    }

    public void addChunk(Chunk chunk) {
        this.chunks.add(chunk);
        this.remainingSize -= chunk.getSize();
        System.out.println("This container has remainingSize: " + this.remainingSize);
    }

    public List<Chunk> getChunkList() {
        return this.chunks;
    }

    public Boolean checkEmptyChunk() {
        Boolean flag = true;
        for (Chunk chunk : this.chunks) {
            if (!chunk.checkCount())
                flag = false;
        }
        return flag;
    }

    public int getRemainingSize() {
        return this.remainingSize;
    }

    public void setFileName(String fn) {
        this.fileName = fn;
    }

    public String getFileName() {
        return this.fileName;
    }

}

public class MyDedup {
    public static final int MAX_CONTAINER_SIZE = 1048576;
    public static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=csci4180p13025;AccountKey=Cj5seNeu7dlIAaEEpI+gicTrVKaJ/pcamqCY3aIvKiqE1mBTGr67VKgo9/EWdSeMZPSaA+V7zx7izkAAiqIAoA==;EndpointSuffix=core.windows.net";

    static {
        System.setProperty("https.proxyHost", "proxy.cse.cuhk.edu.hk");
        System.setProperty("https.proxyPort", "8000");
        System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
        System.setProperty("http.proxyPort", "8000");
    }

    // This Rabin fingerprint has not modular, modular is done in main function
    public static int RFP(byte[] file, int m, int d, int prevRFP, int start) {
        int output = 0;
        int sum = 0;
        if (start == 0 || prevRFP == 0) {
            for (int i = 0; i < m; i++) {
                if (i + start < file.length)
                    sum += file[i + start] * Math.pow(d, m - i - 1);
                else
                    return output;
            }

            output = sum;
        } else {
            if (start + m - 1 < file.length) {
                output = (int) (d * (prevRFP - Math.pow(d, m - 1) * file[start - 1]) + file[start + m - 1]);
            } else
                return output;

        }
        return output;
    }

    // public static void chunking(byte[] file, int m, int d, int q, int max_chunk)
    // {
    // int cut_point = 0;

    // while (cut_point < file.length) {
    // int temp_point = cut_point;
    // int current = m;
    // int fingerPrint = RFP(file, m, d, q, 0, cut_point);
    // while (!((fingerPrint & 0xFF) == 0) || cut_point + m < file.length || current
    // < max_chunk) {
    // temp_point++;
    // fingerPrint = RFP(file, m, d, q, fingerPrint, temp_point);
    // current++;
    // }
    // byte[] data = Arrays.copyOfRange(file, cut_point, cut_point + current);
    // md.update(file, cut_point, cut_point + current);
    // byte[] checksumBytes = md.digest();
    // cut_point += current;

    // //TODO...

    // }

    // }

    public static boolean fileExist(String fileName) {
        File file = new File(fileName);
        if (file.exists())
            return true;
        else
            return false;
    }

    public static void writeFileRecipe(FileRecipe fr) {
        System.out.println("Writing local file recipe...");
        File fileRecipe = new File("./metadata/recipe/FileRecipe-" + fr.getFileName());
        List<FingerprintIndex> fpiList = fr.getFpiList();
        try {
            FileWriter fw = new FileWriter("./metadata/recipe/FileRecipe-" + fr.getFileName());
            String name = fr.getFileName();
            fw.write(name + "\n");
            for (FingerprintIndex fpi : fpiList) {
                fw.write(fpi.toString());
                System.out.println(fpi.toString());
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static FileRecipe loadFileRecipe(String path) {
        System.out.println("Loading local file recipe...");
        File fr = new File(path);
        FileRecipe output = new FileRecipe();
        if (fr.exists()) {
            try {
                // byte[] fileContent = Files.readAllBytes(indexFile.toPath());

                // not sure correct or not
                List<String> data = Files.readAllLines(fr.toPath());
                output.setFileName(data.get(0));
                for (int i = 1; i < data.size(); i++) {
                    FingerprintIndex f = new FingerprintIndex();
                    System.out.println("Raw: " + data.get(i));
                    String[] sections = data.get(i).split("-");
                    f.setChecksum((sections[0]));
                    f.setOffset(Integer.valueOf(sections[1]));
                    f.setContainerFileName(sections[2]);
                    f.setCount(Integer.parseInt(sections[3]));
                    f.setSize(Integer.parseInt(sections[4]));
                    output.addFpi(f);
                    System.out.println("Processed: " + f.toString());
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return output;
        }
        return output;

    }

    public static List<FingerprintIndex> loadLocalIndexFile() {
        System.out.println("Loading local index file...");
        List<FingerprintIndex> fingerprintIndex = new ArrayList<FingerprintIndex>();
        File indexFile = new File("./metadata/mydedup.index");
        if (indexFile.exists()) {

            try {
                // byte[] fileContent = Files.readAllBytes(indexFile.toPath());
                List<String> data = Files.readAllLines(indexFile.toPath());
                for (int i = 0; i < data.size(); i++) {
                    FingerprintIndex f = new FingerprintIndex();
                    System.out.println("Raw: " + data.get(i));
                    // System.out.println("Raw: " + data);
                    String[] sections = data.get(i).split("-");
                    f.setChecksum((sections[0]));
                    f.setOffset(Integer.valueOf(sections[1]));
                    f.setContainerFileName(sections[2]);
                    f.setCount(Integer.parseInt(sections[3]));
                    f.setSize(Integer.parseInt(sections[4]));
                    fingerprintIndex.add(f);
                    System.out.println("Processed: " + f.toString());
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return fingerprintIndex;
        }
        return fingerprintIndex;

    }

    public static void writeLocalIndexFile(List<FingerprintIndex> fingerprintIndex) {
        System.out.println("Writing local index file...");
        File indexFile = new File("./metadata/mydedup.index");
        indexFile.delete();
        try {
            FileWriter fw = new FileWriter("./metadata/mydedup.index");
            for (FingerprintIndex f : fingerprintIndex) {
                if (!f.isCountZero()) {
                    fw.write(f.toString());
                    System.out.println(f.toString());
                }
            }
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void addChunkToContainer(Container con, Chunk chunk, List<FingerprintIndex> fpi, FileRecipe fr) {
        System.out.println("Adding Chunks to Container...");

        FingerprintIndex temp = new FingerprintIndex();
        System.out.println("Fingerprint checksum is: " + chunk.getChecksum());
        // System.out.println(fpi.lastIndexOf(chunk.getChecksum().toString()));
        int fpexist = 0;
        for (FingerprintIndex f : fpi) {
            if (f.getChecksum().equals(chunk.getChecksum())) {
                fpexist = 1;
                f.addCount();
                temp = f;
            }
        }
        if (fpexist == 1) { // if == -1 mean there is checksum already
            System.out.println("Fingerprint is already there...");
            // #TODO: need to update index file below, and add filepath link to the old
            // file...
        } else {
            System.out.println("Adding Fingerprint to fpi...");


            if (con.getRemainingSize() >= chunk.getSize()) {
                con.addChunk(chunk);
            } else {
                // upload the old container
                flushContainer(con);
                // create new container for this chunk
                con.initialize();
                con.addChunk(chunk);
                con.setFileName("container" + (countContainer() + 1));
                System.out.println("The new container has chunk list size " + con.getChunkList().size());
                // ...
                // ...

            }
            temp.setChecksum(chunk.getChecksum());
            temp.setContainerFileName(con.getFileName());
            temp.setOffset(MAX_CONTAINER_SIZE - con.getRemainingSize() - chunk.getSize());
            temp.setCount(1);
            temp.setSize(chunk.getSize());
            fpi.add(temp);
            System.out.println(temp.toString());
        }
        fr.addFpi(temp);
    }

    public static byte[] downloadContainer(String name, int offset, int size) {
        System.out.println("Downloading the container data...");
        // Container con = new Container();
        // Chunk part = new Chunk();
        File container = new File(name);
        byte[] data = new byte[size];
        if (container.exists()) {
            try {
                // Or readallbytes ?
                byte[] bytes = Files.readAllBytes(container.toPath());
                System.out.println(bytes.toString());
                System.out.printf("offset: %d size: %d \n", offset, size);
                data = Arrays.copyOfRange(bytes, offset, offset + size);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return data;
    }

    public static void deleteFileRecipe(FileRecipe fr, List<FingerprintIndex> fpi) {
        System.out.println("Deleting local file recipe...");
        File fileRecipe = new File("./metadata/recipe/FileRecipe-" + fr.getFileName());
        List<FingerprintIndex> fpiList = fr.getFpiList();
        String fp1;
        // List<String> containers = new ArrayList<String>();
        for (FingerprintIndex frIndex : fpiList) {
            System.out.println("Deleting Fingerprint: " + frIndex.toString());
            fp1 = frIndex.getChecksum();
            for (FingerprintIndex fp : fpi) {
                if (fp.getChecksum().equals(fp1)) {
                    System.out.println("Before delete Fingerprint in fpi: " + fp.toString());
                    fp.reduceCount();
                    System.out.println("After delete Fingerprint in fpi: " + fp.toString());
                    break;
                }
            }
        }
        if (fileRecipe.delete()) {
            System.out.println(fileRecipe.getName() + " deleted");
        } else {
            System.out.println("failed");
        }
        // return containers;
    }

    public static List<String> updateContainer(List<FingerprintIndex> fpi) {
        List<FingerprintIndex> toRemove = new ArrayList<FingerprintIndex>();
        List<String> containers = new ArrayList<String>();
        for (FingerprintIndex f : fpi) {
            if (f.isCountZero()) {
                toRemove.add(f);
            }
            if (!containers.contains(f.getContainerFileName())) {
                containers.add(f.getContainerFileName());
            }
        }
        fpi.removeAll(toRemove);
        List<String> containerNotToRemove = new ArrayList<String>();
        for (FingerprintIndex f : fpi) {
            if (!containerNotToRemove.contains(f.getContainerFileName())) {
                containerNotToRemove.add(f.getContainerFileName());
            }
        }
        containers.removeAll(containerNotToRemove);
        return containers;
    }

    public static void flushContainer(Container con) {
        System.out.println("Flushing the container...");
        File container = new File("./data/" + con.getFileName());
        try {
            FileOutputStream os = new FileOutputStream(container);
            for (Chunk chunk : con.getChunkList()) {
                os.write(chunk.getData());
                // System.out.println(chunk.getData().toString());
            }
            os.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void flushContainerToAzure(Container con) {

        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.getContainerReference("container");

            // Create the container if it does not exist.
            container.createIfNotExists();

            CloudBlockBlob blob = container.getBlockBlobReference(con.getFileName());
            File source = new File("./data/" + con.getFileName());
            blob.upload(new FileInputStream(source), source.length());
            source.delete();
            // Delete the blob.
            // blob.deleteIfExists();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String downloadContainerFromAzure(String fn) {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.getContainerReference("container");

            // Create the container if it does not exist.
            // container.createIfNotExists();

            File tmp = new File("./tmp.txt");
            if (tmp.exists()) {
                tmp.delete();
            }

            CloudBlockBlob blob = container.getBlockBlobReference(fn);
            blob.download(new FileOutputStream("./tmp.txt"));

            // Delete the blob.
            // blob.deleteIfExists();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return "./tmp.txt";
    }

    public static void deleteContainerFromAzure(String fn) {
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.getContainerReference("container");

            // Create the container if it does not exist.
            container.createIfNotExists();

            CloudBlockBlob blob = container.getBlockBlobReference(fn);
            // Delete the blob.
            blob.deleteIfExists();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int countContainer() {
        return new File("./data/").list().length;
    }

    public static int countRecipe() {
        return new File("./metadata/recipe/").list().length;
    }

    public static int countTotalChunks(List<FingerprintIndex> fpi) {
        int totalChunks = 0;
        for (FingerprintIndex index : fpi) {
            totalChunks += index.getCount();
        }
        return totalChunks;
    }

    public static int countTotalChunksBytes(List<FingerprintIndex> fpi) {
        int totalChunkBytes = 0;
        for (FingerprintIndex index : fpi) {
            totalChunkBytes += index.getCount() * index.getSize();
        }
        return totalChunkBytes;
    }

    public static int countUniqueChunksBytes(List<FingerprintIndex> fpi) {
        int uniqueChunkBytes = 0;
        for (FingerprintIndex index : fpi) {
            uniqueChunkBytes += index.getSize();
        }
        return uniqueChunkBytes;
    }

    public static void reportStats(List<FingerprintIndex> fpi) {

        int totalContainer = countContainer();
        int totalChunks = countTotalChunks(fpi);
        int totalChunkBytes = countTotalChunksBytes(fpi);
        int uniqueChunkBytes = countUniqueChunksBytes(fpi);
        System.out.println("Report Output: ");
        System.out.printf("Total number of files that have been stored: %d\n", countRecipe());
        System.out.printf("Total number of pre-deduplicated chunks in storage: %d\n", totalChunks);
        System.out.printf("Total number of unique chunks in storage: %d\n", fpi.size());
        System.out.printf("Total number of bytes of pre-deduplicated chunks in storage: %d\n", totalChunkBytes);
        System.out.printf("Total number of bytes of unique chunks in storage: %d\n", uniqueChunkBytes);
        System.out.printf("Total number of containers in storage: %d\n", totalContainer);
        System.out.printf("Deduplication ratio: %f\n", (float) totalChunks / fpi.size());
    }

    public static void initDirectory() {
        File metadataDir = new File("./metadata");
        if (!metadataDir.exists()) {
            metadataDir.mkdirs();
        }
        File metadataRecipeDir = new File("./metadata/recipe");
        if (!metadataRecipeDir.exists()) {
            metadataRecipeDir.mkdirs();
        }
        File dataDir = new File("./data");
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
    }

    public static void main(String[] args) throws Exception {

        initDirectory();

        String operation = args[0];

        if (operation.equals("upload")) {

            int min_chunk = Integer.parseInt(args[1]);
            int avg_chunk = Integer.parseInt(args[2]);
            int max_chunk = Integer.parseInt(args[3]);
            int d = Integer.parseInt(args[4]);
            String file_to_upload = args[5];
            String option = args[6];

            Container con = new Container();
            con.setFileName("container" + (countContainer() + 1));

            if (!fileExist(file_to_upload)) {
                System.out.println("the file to upload does not exist");
                System.exit(1);
            }

            File file = new File(file_to_upload);
            byte[] bytes = Files.readAllBytes(file.toPath());
            List<FingerprintIndex> fpi;
            if (fileExist("./metadata/mydedup.index")) {
                fpi = loadLocalIndexFile();
            } else {
                fpi = new ArrayList<>();
            }
            // int UBytes = 0;
            // The file index that save all chunk info

            int prevRFP = 0;

            int count = 0;
            int cut_point = 0;

            FileRecipe fr = new FileRecipe();
            String[] frName = file_to_upload.split("/");
            fr.setFileName(frName[frName.length - 1]);

            // int last_chunk_fininshed = 0;

            // List<FingerprintIndex> fpi = new ArrayList<FingerprintIndex>();

            for (int i = 0; i < bytes.length; i++) {

                // cut point: prev anchor location
                if (cut_point + min_chunk < bytes.length) {
                    prevRFP = RFP(bytes, min_chunk, d, prevRFP, i);
                    prevRFP = prevRFP % avg_chunk;

                    // chunk size
                    count++;

                    if (((prevRFP & 0xFF) == 0 && count > min_chunk) || count == max_chunk) {
                        System.out.println("Current off set: " + i);
                        MessageDigest md = MessageDigest.getInstance("SHA-256");
                        byte[] data = Arrays.copyOfRange(bytes, cut_point, cut_point + count);
                        md.update(data);
                        byte[] checksumBytes = md.digest();
                        // System.out.println("Checksum: " + Arrays.toString(checksumBytes));
                        StringBuilder sb = new StringBuilder();
                        for (byte b : checksumBytes) {
                            sb.append(String.format("%02x", b));
                        }
                        // System.out.println(sb.toString());
                        cut_point = cut_point + count;
                        // save in some data structure
                        // ...
                        // ...

                        Chunk part = new Chunk();
                        part.setChecksum(sb.toString());
                        part.setData(data);
                        part.setSize(count);
                        part.addCount();

                        addChunkToContainer(con, part, fpi, fr);
                        
                        prevRFP = 0;
                        count = 0;
                        // System.out.println(new String(data));
                    }
                } else {
                    System.out.println("Current off set: " + i);

                    MessageDigest md = MessageDigest.getInstance("SHA-256");
                    byte[] data = Arrays.copyOfRange(bytes, cut_point, bytes.length);
                    md.update(data);
                    byte[] checksumBytes = md.digest();
                    // System.out.println("Checksum: " + Arrays.toString(checksumBytes));
                    StringBuilder sb = new StringBuilder();
                    for (byte b : checksumBytes) {
                        sb.append(String.format("%02x", b));
                    }
                    // System.out.println(sb.toString());

                    Chunk part = new Chunk();
                    part.setChecksum(sb.toString());
                    part.setData(data);
                    part.setSize(bytes.length - cut_point);
                    part.addCount();

                    addChunkToContainer(con, part, fpi, fr);
                    // count = 0;
                    // last_chunk_fininshed = 1;
                    System.out.println("Finished reading whole file. Now process upload");
                    break;
                }

            }
            if (con.getChunkList().size() != 0) {
                flushContainer(con);
            }
            if (option.equals("azure")) {
                flushContainerToAzure(con);
            }
            writeLocalIndexFile(fpi);
            writeFileRecipe(fr);
            // print statistics below
            reportStats(fpi);

        } else if (operation.equals("download")) {
            String file_to_download = args[1];
            String local_file_name = args[2];
            String option = args[3];
            FileRecipe fr = new FileRecipe();
            if (!fileExist("./metadata/recipe/FileRecipe-" + file_to_download)) {
                System.out.println("file does not exist");
                System.exit(1);
            } else {
                // load the file recipe
                fr = loadFileRecipe("./metadata/recipe/FileRecipe-" + file_to_download);
            }

            List<FingerprintIndex> fpi = loadLocalIndexFile();

            List<FingerprintIndex> fr_fpi = fr.getFpiList();

            // idk how to initialize
            // byte[] data = new byte[]();
            // List<Byte> data = new ArrayList<Byte>();
            File output = new File(local_file_name);
            FileOutputStream os = new FileOutputStream(output);

            for (FingerprintIndex x : fr_fpi) {
                String check_sum = x.getChecksum();
                int offset = x.getOffset();
                int size = x.getSize(); 
                String con_name; 
                con_name = x.getContainerFileName();
                if (option.equals("local")) {
                    con_name = "./data/" + con_name;
                } else if (option.equals("azure")) {
                    
                    con_name = downloadContainerFromAzure(con_name);
                }    

                byte[] data = downloadContainer(con_name, offset, size);
                os.write(data);
            }
            os.close();
            
            
        } else if (operation.equals("delete")) {
            String file_to_delete = args[1];
            String option = args[2];
            FileRecipe fr = new FileRecipe();
            String[] frName = file_to_delete.split("/");
            fr.setFileName(frName[frName.length - 1]);

            if (!fileExist("./metadata/recipe/FileRecipe-" + frName[frName.length - 1])) {
                System.out.println("./metadata/recipe/FileRecipe-" + frName[frName.length - 1]);
                System.out.println("File does not exist");
                System.exit(1);
            } else {
                fr = loadFileRecipe("./metadata/recipe/FileRecipe-" + frName[frName.length - 1]);
            }

            List<FingerprintIndex> fpi;
            if (fileExist("./metadata/mydedup.index")) {
                fpi = loadLocalIndexFile();
            } else {
                fpi = new ArrayList<>();
            }
            // editLocalIndexFile(fpi);
            deleteFileRecipe(fr, fpi);
            writeLocalIndexFile(fpi);
            List<String> containersToRemove = updateContainer(fpi);
            if (containersToRemove.size() > 0) {
                // get containers and delete
                if (option.equals("local")) {
                    for (String fn : containersToRemove) {
                        try {
                            File container = new File("./data/" + fn);
                            container.delete();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                } else if (option.equals("azure")) {
                    for (String fn : containersToRemove) {
                        deleteContainerFromAzure(fn);
                    }
                }
            }
            reportStats(fpi);
        }
    }
}