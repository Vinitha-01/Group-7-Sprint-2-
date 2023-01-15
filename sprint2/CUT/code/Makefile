all: dbserver dbclient


dbserver: dbserver.c
	gcc -o dbserver -c dbserver.c -pthread -Walls

dbclient: dbclient.c
	gcc -o dbclient -c dbclient.c -Wall 
