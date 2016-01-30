package uk.vitalcode.events.fetcher.test

import java.io.IOException
import java.net.InetAddress

import org.elasticsearch.ElasticsearchTimeoutException
import org.elasticsearch.action.admin.cluster.health.{ClusterHealthResponse, ClusterHealthStatus}
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.scalatest.{BeforeAndAfterEach, FunSuite, ShouldMatchers}
import uk.vitalcode.events.fetcher.common.Log

class EsTest extends FunSuite with ShouldMatchers with BeforeAndAfterEach with Log {

    var client: Client = _

    test("ES client status tes") {

        val response = waitForCluster(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30), client)

        getClusterStatus should be(ClusterHealthStatus.YELLOW)
    }

    def getClusterStatus: ClusterHealthStatus = {
        client.admin().cluster().prepareHealth().setTimeout(TimeValue.timeValueSeconds(30)).get().getStatus
    }

    @throws(classOf[IOException])
    def waitForCluster(status: ClusterHealthStatus, timeout: TimeValue, client: Client) {
        try {
            log.info(s"Waiting for cluster state [${status.name()}]")
            val healthResponse: ClusterHealthResponse = client
                .admin()
                .cluster()
                .prepareHealth()
                .setWaitForStatus(status)
                .setTimeout(timeout)
                .execute()
                .actionGet()

            if (healthResponse.isTimedOut) {
                throw new IOException(s"Cluster state is " + healthResponse.getStatus.name() + " and not " + status.name()
                    + ", cowardly refusing to continue with operations")
            } else {
                log.info("Cluster state OK")
            }
        } catch {
            case ex: ElasticsearchTimeoutException =>
                throw new IOException("Timeout, cluster does not respond to health request, cowardly refusing to continue with operations")
        }
    }

    override protected def beforeEach(): Unit = {
        val settings: Settings = Settings.settingsBuilder()
            .put("cluster.name", "elasticsearch")
            .build()
        client = TransportClient.builder()
            .settings(settings)
            .build()
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
    }

    override protected def afterEach(): Unit = {
        client.close()
    }
}
