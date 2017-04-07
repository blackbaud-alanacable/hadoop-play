package play

import com.blackbaud.testsupport.BeanCompare
import spock.lang.Specification

public class WordCountSpec extends Specification{

    MiniCluster miniCluster
    

    def setup() {
        miniCluster = new MiniCluster()
    }

    def teardown() {
        miniCluster.stop()
    }

    def "should testClusterWithData"() {
        given:
        miniCluster.writeFile("file01", "Hello World Bye World")
        miniCluster.writeFile("file02", "Hello Hadoop Goodbye Hadoop")
        
        when:
        boolean result = miniCluster.run()

        then:
        assert result
        Map<String, String> map = resultMap(miniCluster.results)
        assert map['Goodbye'] == '1'
        assert map['Hadoop'] == '2'
        assert map['Hello'] == '2'
        assert map['World'] == '2'
    }

    Map<String, Integer> resultMap(String s) {
     return s.split("\\n").collect {
            it.split("\\t")
        }.flatten().toSpreadMap()
    }

}
