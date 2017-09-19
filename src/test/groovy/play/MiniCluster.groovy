package play

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.HdfsConfiguration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.test.PathUtils

public class MiniCluster {

    String clusterName = "cluster1"

    File testDataPath
    Configuration configuration
    MiniDFSCluster cluster
    FileSystem fs

    Path inputDir
    Path outputDir
    MiniCluster() {
        testDataPath = new File(PathUtils.getTestDir(getClass()),
                                "miniclusters")

        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA)
        configuration = new HdfsConfiguration()

        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, new File(testDataPath, clusterName).absolutePath)
        cluster = new MiniDFSCluster.Builder(configuration).build()

        fs = FileSystem.get(configuration)

        inputDir = cleanDirectory("play/input")
        outputDir = cleanDirectory("play/output")
    }

    void stop() {
        Path dataDir = new Path(testDataPath.getParentFile().getParentFile().getParent())
        fs.delete(dataDir, true)
        File rootTestFile = new File(testDataPath.getParentFile().getParentFile().getParent())
        String rootTestDir = rootTestFile.getAbsolutePath()
        Path rootTestPath = new Path(rootTestDir)
        LocalFileSystem localFileSystem = FileSystem.getLocal(configuration)
        localFileSystem.delete(rootTestPath, true)
        cluster.shutdown()
    }

    boolean run() {
        Job job = WordCount.createJob(configuration, inputDir, outputDir)
        job.waitForCompletion(true)
    }

    void writeFile(String fileName, String content) {
        writeHDFSContent(inputDir, fileName, content);
    }

    String getResults() {
        String result = ""
        FileStatus file = fs.listStatus(outputDir).find { it.path.name.contains("part-r-00000") }
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.path)))
        result = reader.text
        reader.close()
        return result
    }


    private Path cleanDirectory(String name) {
        Path dir = new Path(name)
        fs.delete(dir, true)
        dir
    }


    private void writeHDFSContent(Path dir, String fileName, String content) {
        FSDataOutputStream out = fs.create(new Path(dir, fileName))
        out.writeBytes(content)
        out.close()
    }
}