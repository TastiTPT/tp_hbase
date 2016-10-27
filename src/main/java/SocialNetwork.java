/**
 * Created by nicolas on 21/10/16.
 */


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Scanner;
import java.util.List;
import java.util.ArrayList;



public class SocialNetwork {
    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);
        try {


            Table table = connection.getTable(TableName.valueOf("ntastevinHTable"));
            // the name of my table created previously in hbase shell (create 'ntastevinHTable', 'info', 'friends')
            try {





                // firstname query ***************
                // ask the firstname of the user
                Scanner sc = new Scanner(System.in);
                System.out.println("What is your firstname?");
                String firstname = sc.nextLine();
                while(firstname.equals("")){ // to ensure the user types his firstname
                    System.out.println("No firstname Typed. Please type your firstname!:  ");
                    firstname = sc.nextLine();
                }
                // end firstname query ************




                // info query ***********************
                System.out.println("Do you have an info about you to communicate? (y/n)");
                String infoOrNot = sc.nextLine();


                // y/n
                while( (!infoOrNot.equals("y")) && (!infoOrNot.equals("n")) ){ // to ensure the user answers by y or n
                    System.out.println("Please answer with: (y/n)");
                    infoOrNot = sc.nextLine();
                }
                //end y/n

                // other info to communicate ******************
                while(infoOrNot.equals("y")){ // while the user has an info to communicate re do

                    if(infoOrNot.equals("y")){
                        System.out.println("What kind of info is it? (suggestions: mail, phone number, address ...)");
                        String information = sc.nextLine();
                        while(information.equals("")){ // to ensure to get an answer
                            System.out.println("Nothing typed. Please type an answer!:  ");
                            information = sc.nextLine();
                        }
                        System.out.println("Now write the " + information +":");
                        String value = sc.nextLine();
                        while(value.equals("")){ // to ensure to get the value
                            System.out.println("Nothing typed. Please type the value!:  ");
                            value = sc.nextLine();
                        }
                        // fill the table with the firstname as row id and column family info:
                        Put p = new Put(Bytes.toBytes(firstname));
                        p.add(Bytes.toBytes("info"), Bytes.toBytes(information),
                                Bytes.toBytes(value));
                        table.put(p);
                        // end fill table

                    }

                    System.out.println("Do you have an other info about you to communicate? (y/n)");
                    infoOrNot = sc.nextLine();
                    while( (!infoOrNot.equals("y")) && (!infoOrNot.equals("n")) ){ // to ensure to get an answer with y or n
                        System.out.println("Please answer with: (y/n)");
                        infoOrNot = sc.nextLine();
                    }
                }
                // **** end other info to communicate *************************
                // end info query *********************************************






                // best friend query ***************************************
                System.out.println("What is your best friend's firstname?");
                String bestFriend = sc.nextLine();
                while(bestFriend.equals("")){ // to answer the user types a best friend surname
                    System.out.println("No best friend's firstname Typed. Please type it!:  ");
                    bestFriend = sc.nextLine();
                }
                // end best friend query************************************



                // best friend filling table ***

                // 1) fill the table with the best friend as row id if not exist
                Get g = new Get(Bytes.toBytes(bestFriend));
                Result r = table.get(g);
                byte [] rBFF = r.getValue(Bytes.toBytes("friends")
                        ,Bytes.toBytes("BFF"));

                if(rBFF==null) { // fill only is the best Friend firstname does not exist in the table
                    // fill the table with the best friend as row id and column family friends:BFF:
                    Put p = new Put(Bytes.toBytes(bestFriend));
                    p.add(Bytes.toBytes("friends"), Bytes.toBytes("BFF"),
                            Bytes.toBytes(firstname)); // by default set the best friend of the best friend to the name of the user
                    table.put(p);
                    // end fill bestfriend if not exist
                }


                // 2) fill the table with the firstname as row id and column family friends:BFF:
                Put p2 = new Put(Bytes.toBytes(firstname));
                p2.add(Bytes.toBytes("friends"), Bytes.toBytes("BFF"),
                        Bytes.toBytes(bestFriend));
                table.put(p2);
                // end fill table

                // end best friend filling table ***




                // others friends query ********
                List<String> friends = new ArrayList<String>();

                int atLeastOneOther = 0; // to record if the user has at least one other friend

                String friendsOrNot = "y"; // set to "y" to ensure to enter the while loop
                while(friendsOrNot.equals("y")){
                    System.out.println("Do you have an other friend? (y/n)");
                    friendsOrNot = sc.nextLine();
                    while( (!friendsOrNot.equals("y")) && (!friendsOrNot.equals("n")) ){ // to ensure to get an answer with y or n
                        System.out.println("Please answer with: (y/n)");
                        friendsOrNot = sc.nextLine();
                    }
                    if(friendsOrNot.equals("y")){
                        atLeastOneOther += 1;
                        System.out.println("Now write the firstname of your other friend :");
                        String friend = sc.nextLine();
                        while(friend.equals("")){ // to ensure to get a firstname
                            System.out.println("Nothing typed. Please type the firstname of your friend!:  ");
                            friend = sc.nextLine();
                        }
                        friends.add(friend);
                    }

                }

                // end others friends query

                // filling other friends if at least one other friends has been communicated by the user

                if(atLeastOneOther!=0) { // to known if there is an other friend
                    // here we concatenate the names into one string
                    String friendsString = "";
                    for (String s : friends) {
                        friendsString += s + ", ";
                    }
                    String friendsStringFinal = friendsString.substring(0, friendsString.lastIndexOf(", ")); // to remove the ", " at the end of the concatenation

                    // fill the table with the others friend as row id if not exist
                    for (String f : friends) {
                        Get gf = new Get(Bytes.toBytes(f));
                        Result rf = table.get(gf);
                        byte[] rfBFF = rf.getValue(Bytes.toBytes("friends")
                                , Bytes.toBytes("BFF"));

                        if (rfBFF == null) { // fill only is the Friend firstname does not exist in the table
                            // fill the table with the best friend as row id and column family friends:BFF:
                            Put p = new Put(Bytes.toBytes(f));
                            p.add(Bytes.toBytes("friends"), Bytes.toBytes("BFF"),
                                    Bytes.toBytes(firstname)); // by default set the best friend of the friend to the name of the user
                            table.put(p);

                        }
                    } // end fill others friend if not exist


                    // fill the table with the firstname as row id and column family friends:others:
                    Put p = new Put(Bytes.toBytes(firstname));
                    p.add(Bytes.toBytes("friends"), Bytes.toBytes("others"),
                            Bytes.toBytes(friendsStringFinal));
                    table.put(p);
                    // end fill table

                }


                // get the name of the bestfriend of the user to print it to the user shell

                Get yourRow = new Get(Bytes.toBytes(firstname));
                Result yRow = table.get(yourRow);


                byte [] value = yRow.getValue(Bytes.toBytes("friends")
                        ,Bytes.toBytes("BFF"));
                        String valueStr = Bytes.toString(value);

                System.out.println("Thank you, your job is now finish");
                System.out.println("Have a nice day with your best friend: "+valueStr);


            } finally {
                if (table != null) table.close();
            }
        } finally {
            connection.close();
        }
    }
}

