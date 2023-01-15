/*File Name: dbserver.c
 *Description:
 *      Client Side Programming
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string.h>

#include "../include/common.h"

void connectServer(char *hostname, int port);
void startData(int socketfd);

int main(int argc, char **argv)
{
    //Checking number of arguments given at command line
    if (argc < 3)
    {

        printf("Usage: ./dbclient [HOSTNAME] [PORT]");
        return 1;
    }

    connectServer(argv[1], atoi(argv[2]));

    return 0;
}

/*Function Name: connectSerevr()
 *Description:
 *      connecting client to server using IP address and port number
 */
void connectServer(char *hostname, int port)
{

    // Create socket
    int socketfd = socket(AF_INET, SOCK_STREAM, 0);
    if (socketfd == -1)
    {

        perror("Could not create server socket");
        exit(1);
    }

    // Create socket information
    struct addrinfo *res;
    struct addrinfo hints;

    memset(&hints, 0, sizeof(hints));

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(hostname, NULL, &hints, &res) != 0)
    {

        perror("Could not resolve hostname");
        exit(1);
    }

    struct sockaddr_in socketInfo = *((struct sockaddr_in *)(res->ai_addr));
    socketInfo.sin_port = htons(port);

    // Connect to server
    if (connect(socketfd, (struct sockaddr *)&socketInfo, sizeof(socketInfo)) == -1)
    {

        perror("Failed to connect to socket");
        exit(1);
    }

    // Database Shell
    startData(socketfd);
}

/*function Name: startData()
 *Description:
 *      Handling database interactions
 */
void startData(int socketfd)
{

    //Initializing buffer for menu driven interface
    char *action="PUT";
    char *buffer = (char *)malloc(sizeof(char));
    size_t bufferSize = sizeof(buffer);

    while (strcmp(buffer, "0") != 0)
    {
        //User can choose the option
        printf("Enter Your Choice (1 PUT, 2 GET, 3 DELETE, 0 QUIT): ");
        getline(&buffer, &bufferSize, stdin);
        buffer[strlen(buffer) - 1] = '\0';

        // PUT 
        if (strcmp(buffer, "1") == 0)
        {

            char *name = (char *)malloc(sizeof(char) * 128);
            size_t nameSize = sizeof(name);
            //Giving data
            printf("Enter Name: ");
            getline(&name, &nameSize, stdin);
            name[strlen(name) - 1] = '\0';

            char *id = (char *)malloc(sizeof(char) * 32);
            size_t idSize = sizeof(id);

            printf("Enter ID: ");
            getline(&id, &idSize, stdin);
            id[strlen(id) - 1] = '\0';
            //Creating a structure to write data to server
            struct msg writeMsg;
            writeMsg.type = 1;
            strcpy(writeMsg.rd.name, name);
            writeMsg.rd.id = (uint32_t)strtoul(id, NULL, 10);
            //writing the data to server through socket
            write(socketfd, &writeMsg, sizeof(writeMsg));

            free(name);
            free(id);
            action="PUT";

        }

        // Retrieving data using Id
        else if (strcmp(buffer, "2") == 0)
        {

            char *id = (char *)malloc(sizeof(char) * 32);
            size_t idSize = sizeof(id);

            printf("Enter ID: ");
            getline(&id, &idSize, stdin);
            id[strlen(id) - 1] = '\0';

            struct msg writeMsg;
            writeMsg.type = 2;
            writeMsg.rd.id = (uint32_t)strtoul(id, NULL, 10);
            //write the id to server 
            write(socketfd, &writeMsg, sizeof(writeMsg));

            free(id);
            //structute to read the message from the database
            struct msg serverMsg;
            read(socketfd, &serverMsg, sizeof(serverMsg));
            if (!serverMsg.rd.id == 0)
            {

                printf("Name: %s\nID: %u\n", serverMsg.rd.name, serverMsg.rd.id);
            }
            else
            {

                printf("Record Not Found\n");
            }

            continue;
        }

        // Delete
        else if (strcmp(buffer, "3") == 0)
        {

            char *id = (char *)malloc(sizeof(char) * 32);
            size_t idSize = sizeof(id);
            //Enter the id to delete the data related to that id
            printf("Enter ID: ");
            getline(&id, &idSize, stdin);
            id[strlen(id) - 1] = '\0';

            struct msg writeMsg;
            writeMsg.type = 3;
            writeMsg.rd.id = (uint32_t)strtoul(id, NULL, 10);

            write(socketfd, &writeMsg, sizeof(writeMsg));

            free(id);

            action = "DEL";
        }
        else if (strcmp(buffer, "0") == 0)
        {

            break;
        }

        // Read status
        struct msg serverMsg;
        read(socketfd, &serverMsg, sizeof(serverMsg));
        if (serverMsg.type == 4)
        {

            printf("%s Successful\n", action);
        }
        else
        {

            printf("%s Failed\n", action);
        }
    }

    free(buffer);
    close(socketfd);
}