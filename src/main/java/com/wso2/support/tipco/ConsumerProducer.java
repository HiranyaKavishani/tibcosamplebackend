package com.wso2.support.tipco;
import javax.jms.*;
import com.tibco.tibjms.Tibjms;
import com.tibco.tibjms.TibjmsTextMessage;
import java.util.Vector;

public class ConsumerProducer implements ExceptionListener {

    private boolean useTopic   = false;
    private int ackMode = Session.AUTO_ACKNOWLEDGE;
    private String corelation_id = null;
    private Session consumer_session = null;
    private Session producer_session = null;
    private MessageProducer msgProducer = null;
    private Vector<String> data = new Vector<String>();


    private ConsumerProducer(){
        try
        {
            String serverUrl = "tcp://127.0.0.1:7222";
            ConnectionFactory consumer_factory = new com.tibco.tibjms.TibjmsConnectionFactory(serverUrl);
            /* create the connection */
            String userName = "admin";
            String password = "";
            Connection consumer_connection = consumer_factory.createConnection(userName, password);
            /* create the session */
            consumer_session = consumer_connection.createSession(false, ackMode);
            /* set the exception listener */
            consumer_connection.setExceptionListener(this);
            /* start the connection */
            consumer_connection.start();


            ConnectionFactory producer_factory = new com.tibco.tibjms.TibjmsConnectionFactory(serverUrl);
            Connection producer_connection = producer_factory.createConnection(userName, password);
            /* create the session */
            producer_session = producer_connection.createSession(false,javax.jms.Session.AUTO_ACKNOWLEDGE);
            producer_connection.start();
            Consume();
        }
        catch (JMSException e)
        {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        new ConsumerProducer();
    }
    private void Consume() throws JMSException
    {
        Message msg;
        String consumer_queue_name = "Request";
        System.err.println("Subscribing to destination: "+ consumer_queue_name +"\n");
        /* create the destination */
        Destination consumer_destination = null;
        if (useTopic)
            consumer_destination = consumer_session.createTopic(consumer_queue_name);
        else
            consumer_destination = consumer_session.createQueue(consumer_queue_name);
        /* create the consumer */
        MessageConsumer msgConsumer = consumer_session.createConsumer(consumer_destination);
        /* read messages */
        while (true) {
            /* receive the message */
            msg = msgConsumer.receive();
            //corelation_id = msg.getJMSCorrelationID();
            corelation_id = msg.getJMSMessageID();
            System.err.println(System.currentTimeMillis() + "++++++++++++++++++++" + corelation_id +
                    "++++++++++++++++++++++++++++++++" + "message recieved");
            if (ackMode == Session.CLIENT_ACKNOWLEDGE ||
                    ackMode == Tibjms.EXPLICIT_CLIENT_ACKNOWLEDGE ||
                    ackMode == Tibjms.EXPLICIT_CLIENT_DUPS_OK_ACKNOWLEDGE) {
                msg.acknowledge();
            }
            data.clear();
            data.add(((TibjmsTextMessage) msg).getText());
            System.err.println("is get delivery ++++++++++++++++++" + msg.getJMSRedelivered());
            publish(msg.getJMSReplyTo());
        }
    }
    private void publish(Destination replyDest){
        System.err.println("reply destination++++++++++++++++++++++++++++"+replyDest.toString());
        try
        {
            TextMessage msg;
            int         i;
            if (data.size() == 0)
            {
                System.err.println("***Error: must specify at least one message text\n");
            }
            System.err.println("Publishing to destination '"+replyDest);
            /* create the destination */
            String producer_queue_name = "Response";
            if (useTopic)
                producer_session.createTopic(producer_queue_name);
            else {
                producer_session.createQueue(replyDest.toString());
                /* create the producer */
                msgProducer = producer_session.createProducer(replyDest);
            }
            /* publish messages */
            for (i = 0; i<data.size(); i++)
            {
                /* create text message */
                msg = producer_session.createTextMessage();
                msg.setJMSCorrelationID(corelation_id);
                /* set message text */
                msg.setText(data.elementAt(i));
                String textXml = "application/xml";
                msg.setStringProperty("Content_Type", textXml);
                /* publish message */
                try {
                    Thread.sleep(300);
                }catch (Exception e){
                    System.err.println(e);
                }
                msgProducer.send(msg);
                System.err.println(System.currentTimeMillis()+"+++++++++++++++++++++++++++++++"+ corelation_id +
                        "++++++++++++++++++++++++++++++++++++ message published \n");
            }
            msgProducer.close();
        }
        catch (JMSException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
    }
    public void onException(JMSException e)
    {
        /* print the connection exception status */
        System.err.println("CONNECTION EXCEPTION: " + e.getMessage());
    }
}