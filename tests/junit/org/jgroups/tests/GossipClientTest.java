
package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.stack.GossipClient;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.IpAddress;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Util;
import org.testng.annotations.*;

import java.util.List;

/**
 * Tests Gossip protocol primitives with the new GossipRouter. Since 2.2.1, the
 * GossipRouter is supposed to answer Gossip requests too.
 *
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @author Bela Ban
 * @version $Id: GossipClientTest.java,v 1.4 2008/09/10 15:41:01 vlada Exp $
 * @since 2.2.1
 */
@Test(groups=Global.STACK_INDEPENDENT,sequential=true)
public class GossipClientTest extends ChannelTestBase{
    GossipClient client;
    private int port=-1;
    private long expiryTime=1000;
    GossipRouter router;



    @BeforeClass
    void setUp() throws Exception {
        router = createGossipRouter(getBindAddress(),expiryTime);        
        port=router.getPort();
        client=new GossipClient(new IpAddress(getBindAddress(), port), expiryTime, 1000, null);
        client.setRefresherEnabled(false); // don't refresh the registrations
    }

    @AfterClass
    void tearDown() throws Exception {
        client.stop();
        router.stop();
    }


    public void testEmptyGET() throws Exception {
        String groupName="nosuchgroup";
        List mbrs=client.getMembers(groupName);
        assert mbrs != null;
        assert mbrs.isEmpty();
    }


    /**
     * Registers an address with a group and then sends a GET request for that group.
     */
    public void test_REGISTER_GET() throws Exception {
        String groupName="TESTGROUP";
        int mbrPort=7777;
        Address mbr=new IpAddress(getBindAddress(), mbrPort);
        client.register(groupName, mbr, true);

        List mbrs=client.getMembers(groupName);
        assert mbrs.size() == 1;
        assert mbrs.get(0).equals(new IpAddress(getBindAddress(), mbrPort));
    }

    public void test_REGISTER_UNREGISTER_GET() throws Exception {
        String groupName="TESTGROUP-2";
        int mbrPort=7777;
        Address mbr=new IpAddress(getBindAddress(), mbrPort);
        client.register(groupName, mbr);

        List mbrs=client.getMembers(groupName);
        assert mbrs.size() == 1;
        assert mbrs.get(0).equals(new IpAddress(getBindAddress(), mbrPort));

        client.unregister(groupName, mbr);// done asynchronous, on a separate thread
        Util.sleep(500);
        mbrs=client.getMembers(groupName);
        assert mbrs != null;
        assert mbrs.isEmpty();
    }


    /**
     * Test if a member is removed from group after EXPIRY_TIME ms.
     */
    public void testSweep() throws Exception {
        String groupName="TESTGROUP-3";
        int mbrPort=7777;
        Address mbr=new IpAddress(getBindAddress(), mbrPort);

        client.register(groupName, mbr);

        Util.sleep(500);
        List mbrs=client.getMembers(groupName);
        assert mbrs.size() == 1;
        assert mbrs.get(0).equals(new IpAddress(getBindAddress(), mbrPort));

        // because the sweep is ran at fixed expiryTime intervals, if
        // an entry was added immediately after a sweep run, it actually 
        // spends almost 2*expiryTime in cache.
        Thread.sleep(2 * expiryTime);

        // send a second GET after more than EXPIRY_TIME ms
        mbrs=client.getMembers(groupName);
        assert mbrs == null || mbrs.isEmpty();
    }

  


}