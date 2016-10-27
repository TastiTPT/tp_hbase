# Tp hbase


### Goal

The SocialNetwork.java performs a REPL which asks the user to enter his name.  
Then the user is invited to add more information about him.  
The REPL forces the user to enter the first name of his best friend.  
All information are stored into a hbase table.  
The REPL ensures that the best friend and the others friends the user communicate will have their own row id.  
If they are already in the htable, nothing is done.  
If they did not have a row id, then  rows id with the proper firstnames are created and by default the best friend is the firstname of the user.



### dependency
```
  org.apache.hbase.hbase-client.1.1.2
```


## Prerequisite

Assuming that a hbase table called <ntastevinHTable> has been previously created with base shell with the two following columns family: <info> and <friends>

```
hbase shell > create 'ntastevinHTable', 'info', 'friends'

```

## Authors

* **Nicolas Tastevin** 
