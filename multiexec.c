#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <string.h>
#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
                
//#define DEBUG 
#define USESCHED

#define TIMEOUT	60
#define MAXPROC 10
#define RETRY 1
#define OUTPUTTERSE 0
#define OUTPUTVERBOSE 1
#define SUMMARY 0

struct childprocess 
{
  pid_t pid;
  int exited;
  int termed;
  time_t starttime;
  time_t timeout;
  unsigned int retrycount;
  char *outputfilename;
  FILE *outputfile;
  char *label;
  char *command;
  struct childprocess *next;
  struct childprocess *prev;
};

struct childprocess head;
struct childprocess tail;
struct childprocess failedhead;
struct childprocess failedtail;

char outfilebase[]="/tmp/multiexec";

char exitproc=0;
unsigned long listlen=0;
unsigned long childcount=0;

void dcreatechildnode(struct childprocess *temp)
{
#ifdef DEBUG
  printf("entering dcreatechild\n");
#endif
  if (temp)
  {
    if (temp->outputfile)
    {
      fclose((FILE *)temp->outputfile);
      temp->outputfile=NULL;
    }    
    if (temp->outputfilename)
    {
      if (!temp->exited && temp->pid)
      {
        kill(temp->pid,SIGKILL);
        unlink(temp->outputfilename);
      }
      free(temp->outputfilename);
      temp->outputfilename=NULL;
    }
    if (temp->label)
    {
      free(temp->label);
      temp->label=NULL;
    }
    if (temp->command)
    {
      free(temp->command);
      temp->command=NULL;
    }
    free(temp);
    temp=NULL;
  }
#ifdef DEBUG
  printf("leaving dcreatechildnode\n");
#endif
}

struct childprocess *createchildnode(time_t timeout,char *label,char *command)
{
  struct childprocess *temp;
  
#ifdef DEBUG
  printf("Entering createchildnode\n");
#endif
  if (temp=malloc(sizeof(struct childprocess)))
  {
    temp->starttime=time(NULL);
    temp->timeout=timeout;
    temp->retrycount=0;
    temp->exited=NULL;
    temp->pid=0;
    temp->outputfilename=NULL;
    temp->outputfile=NULL;
    if (!(temp->label=strdup(label))) 
    {
#ifdef DEBUG
      printf("Exiting createchildnode, unable to allocate string for label\n");
#endif
      dcreatechildnode(temp);
      return(NULL);
    }
    if (!(temp->command=strdup(command))) 
    {
#ifdef DEBUG
      printf("Exiting createchildnode, unable to allocate space for command string\n");
#endif
      dcreatechildnode(temp);
      return(NULL);
    }
    temp->prev=NULL;
    temp->next=NULL;
  }
#ifdef DEBUG
  else
  {
    printf("Exiting createchildnode, unable to malloc childnode\n");
    return(NULL);
  }
#endif
  // Something went wrong, clean up and return error
#ifdef DEBUG
  printf("Exiting createchildnode, success\n");
#endif
  return(temp);
}  

void forkchild(struct childprocess *node)
{
  char outfile[]="/tmp/multiexec.000000.000000";
  int descin;
  int descout;
  pid_t parentpid;
  int pipefd[2];
  char buffer[10];
  parentpid=getpid();
  
  // Create the pipe used to syncronize the parent and child process
  // Witout this it is possible for the child to exit before the parent
  // Receives the child's pid.
  // This causes the signal handler that handles child death to not
  // remove the child process from the process list because the process
  // in question hasn't had it's pid updated.
  pipe(pipefd);
  
  // Fork
  node->pid=fork();
#ifdef DEBUG
  printf("Entering forkchild\n");
#endif
  switch(node->pid)
  {
      case 0:
        // We are the child process
        
        // Attach stdin to /dev/null
        descin=open("/dev/null",O_RDWR);
        close(0);
        dup2(descin,0);
#ifdef DEBUG
        printf("child: Executing command\n");
#endif
        // Create the outfile and attach stdout and stderr to it
        sprintf(outfile,"%s.%.6d.%.6d",outfilebase,parentpid,getpid());
        descout=open(outfile,O_RDWR|O_CREAT,448);
        close(1);
        close(2);
        dup2(descout,1);
        dup2(descout,2);

        // Wait until we know the parent is ready
        close(pipefd[1]);
        // We don't need to run before the parent stores are pid so we
        // give up our timeslice to allow the parent a chance to run.
#ifdef USESCHED
        sched_yield();
#endif
        read(pipefd[0],buffer,2);
        close(pipefd[0]);
        
        // Exec the command
        if (node->command)
        {
          // The following will replace our process with the command specified
          //sleep(1);
          execl("/bin/sh","sh","-c",node->command,(char *)0);
          _exit(127);
        }

        // Kludge to keep the child from exiting before the parent puts the pid in it's process list
        // This should never get called since we changed from using system() to execl()
        //sleep(1);
        exit(0);
      case -1:
        // Fork returned and error
        printf("forkchild: fork returned an error\n");
        close(pipefd[1]);
        close(pipefd[0]);
        break;
      default:
#ifdef DEBUG
        printf("forkchild: forked off command %s, pid %d\n", node->command,node->pid);
#endif
        // We aren't going to read from the pipe
        // so we close the reading end.
        close(pipefd[0]);
        
        // We are the parent process
        // return the process scruct to the caller
        sprintf(outfile,"%s.%.6d.%.6d",outfilebase,parentpid,node->pid);
#ifdef DEBUG
        printf("forkchild: waiting for child %d to signal ready\n",node->pid);
#endif
        write(pipefd[1],"ok",2);
        close(pipefd[1]);
      
#ifdef DEBUG
        printf("forkchild: child %d has started\n",node->pid);
#endif  
        // We are the parent process
        // return the process scruct to the caller
        sprintf(outfile,"%s.%.6d.%.6d",outfilebase,parentpid,node->pid);
        if (!(node->outputfilename=strdup(outfile)))
        {
          perror("forkchild: unable to allocate memory\n");
          dcreatechildnode(node);
          return;
        }
        node->starttime=time(NULL);
        node->timeout=node->timeout+time(NULL);
        childcount++;
#ifdef DEBUG
        printf("Leaving forkchild, good return code\n");
#endif
        return;
  }
}

void removelinefeeds(char *line)
{
  int lenline;
  int count;
  
  lenline=strlen(line);
  for (count=0;count < lenline;count++)
  {
    if (line[count]=='\n' || line[count]=='\r')
    {
      line[count]='\0';
      break;
    }
  }
}

char *getcommand(char *buffer)
{
  int lenbuf;
  int count;
  char *command=NULL;
  
  lenbuf=strlen(buffer);
  command=buffer;
  for (count=0;count < lenbuf;count ++)
  {
    if (buffer[count] == ' ')
    {
      command=(&buffer[count])+1;
      break;
    }
  } 
  return(command);
}

char *getlabel(char *buffer,char *label)
{
  int lenbuf;
  int count;
  int foundtoken=0;
  
  lenbuf=strlen(buffer);
  
  for (count=0;count < lenbuf;count ++)
  {
    if (buffer[count] == ' ')
    {
      label[count]='\0';
      foundtoken=1;
      break;
    }
    label[count]=buffer[count];
  }  
  if (!foundtoken) label[0]='\0';
  return(label);
}

void newlist(struct childprocess *head, struct childprocess *tail)
{
  head->next=tail;
  head->prev=head;
  tail->next=tail;
  tail->prev=head;
  listlen=0;
  return;
}

void appendprocess(struct childprocess *process,struct childprocess *tail)
{
  tail->prev->next=process;
  process->prev=tail->prev;
  process->next=tail;
  tail->prev=process;
  listlen++;
}

void removeprocess(struct childprocess *process)
{
  if (process == &head)
  {
    printf("removeprocess: attempt to remove the head node\n");
    return;
  }
  if (process == &tail)
  {
    printf("removeprocess: attempt to remove the tail node\n");
  }
#ifdef DEBUG
  printf("removeprocess: Removing command %s, pid %d\n",process->command,process->pid);
#endif
  process->prev->next=process->next;
  process->next->prev=process->prev;
  listlen--;  
}

struct childprocess *findnodebypid(struct childprocess *head,pid_t pid)
{
  struct childprocess *node;
  
#ifdef DEBUG
  printf("findnodebypid: entering\n");
#endif
  node=head->next;
  while (node != node->next)
  {
    if (node->pid == pid) 
    {
#ifdef DEBUG
      printf("findnodebypid: exiting, found node\n");
#endif
      return(node);
    }
    node=node->next;
  }
#ifdef DEBUG
  printf("findnodebypid: exiting, no node found\n");
#endif
  return(NULL);
}

int emptylist(struct childprocess *head)
{
  if (head->next->next == head->next) return(1);
  return(0);
}

void killallchildren(struct childprocess *head, struct childprocess *tail)
{
  struct childprocess *node;
  struct childprocess *next;
  
  node=head->next;
  while (node != node->next)
  {
    next=node->next;
#ifdef DEBUG
    printf("deleting child: %s\n",node->command);
#endif
    dcreatechildnode(node);
    node=next;
  }
  newlist(head,tail);
}

void printlist(struct childprocess *head)
{
#ifdef DEBUG  
  node=head->next;
  while (node != node->next)
  {
    printf("node: %d\t%s\t%s\t%s\n",node->pid,node->label,node->command,node->outputfilename);
    node=node->next;
  }
#endif
}

/* Print all output from a process node (if any) */
void printoutput(struct childprocess *node,int format,int timeout)
{
  FILE *fh;
  char buffer[8192];

#ifdef DEBUG
  printf("Entering printoutput for pid %d\n",node->pid);
#endif
  if (node->outputfilename)
  {
#ifdef DEBUG
    printf("Node temp file is: %s\n",node->outputfilename); 
#endif
    if (fh=fopen(node->outputfilename,"r"))
    {
#ifdef DEBUG
      printf("Printing output -----------------\n");
#endif
      switch (format)
      {
        case OUTPUTTERSE:
          if (timeout) printf("%s: TIMEOUT (%d)\n",node->label,node->pid);
          while (fgets(buffer,8191,fh)!=NULL)
          {
            printf("%s: %s",node->label,buffer);
          }
          break;
        case OUTPUTVERBOSE:
          printf("-----------------------------------------------------\n");
          if (timeout) printf("%s: %s (TIMEOUT)\n",node->label,node->command);
          else printf("%s: %s\n",node->label,node->command);
          while (fgets(buffer,8191,fh)!=NULL)
          {
            printf("%s",buffer);
          }
          break;
      }
#ifdef DEBUG
      printf("Done printing output ------------\n");
#endif
      fclose(fh);
    } 
  }
}

void printalloutput(struct childprocess *head,int format, unsigned int argtimeout)
{
  struct childprocess *node;
  struct childprocess *tmpnode;
  
#ifdef DEBUG
  printf("Entering printalloutput\n");
#endif
  node=head->next;
  while (node != node->next)
  {
    tmpnode=node->next;
    if (node->exited) 
    {
      removeprocess(node);
      printoutput(node,format,0);
      dcreatechildnode(node);
    }
    else if ((time(NULL)) > (node->timeout)&&(node->retrycount<RETRY)) 
    {
      // If we haven't hit the number of max retries kill the old process, update retrycount and timeout and refork.
      // We must remove the node from the list, so the signal handler doesn't mark it as exited when we kill it.
#ifdef DEBUG
      printf("printalloutput: retrying %s: %s, %d\n",node->label,node->command,listlen);
#endif
      removeprocess(node);
      if (node->pid > 1)
      {
        kill(node->pid,SIGKILL);
        unlink(node->outputfilename);
        free(node->outputfilename);
        node->outputfilename=NULL;
        
      }
#ifdef USESCHED
      // We do this because often times commands will fail due to resource issues giving up our timeslice
      // helps with this.
      sched_yield();
#endif
      node->retrycount++;
      node->exited=0;
      forkchild(node);
      node->timeout=time(NULL)+argtimeout;
      appendprocess(node,&tail);      
    }
    else if ((time(NULL) > node->timeout)&&(node->retrycount>=RETRY))
    {
#ifdef DEBUG
      printf("printalloutput: Timeout %s: %s\n",node->label,node->command);
#endif
      printoutput(node,format,1);
      removeprocess(node);
      appendprocess(node,&failedtail);
      //dcreatechildnode(node);
    }
    node=tmpnode;
  }
#ifdef DEBUG
  printf("Leaving printalloutput\n");
#endif
}

/* Handle the exiting of one of our children */
void sig_chld(int signo) 
{
    int status;
    pid_t childpid;
    struct childprocess *node;    

#ifdef  DEBUG
  printf("Entering sig_chld()\n");
#endif    
    while(1)
    {
    /* Get the exiting child's pid and exit status */
#ifdef DEBUG
      printf("Waiting for signal\n");
#endif
      childpid=waitpid(-1,&status,WNOHANG);
      if (childpid<0)
      {
        //perror("waitpid failed");
        break;
      }
      if (childpid==0) break;
#ifdef DEBUG
      printf("signal #: %d\tpid: %d\n",signo,childpid);
#endif 

    /* Get the childprocess node for the pid */
#ifdef DEBUG
      printf("sig_child: finding process node for signal\n");
      //printlist(&head);
#endif
      if (node=findnodebypid(&head,childpid))
      {
      /* Mark the process as exited */
#ifdef DEBUG
        printf("sig_child: marked process node as exited\n");
#endif
        node->exited=1;
      }
#ifdef DEBUG
      // This can happen if the node was already removed due to timeout
      else
      {
        printf("sig_chld: Error, signal handler called for pid %d without process\n",childpid);
        //printlist(&head);
      }
#endif
    }
    exitproc=1;
#ifdef DEBUG
  printf("Exiting sig_chld()\n");
#endif
}
                                                                                                                                                                                                                                                              
int main(int argc, char **argv)
{
  struct childprocess *child=NULL;
  char buffer[8192];
  char label[8192];
  struct sigaction act, oldact;
  unsigned int maxproc=MAXPROC;
  unsigned int argtimeout=TIMEOUT;
  unsigned int summary=SUMMARY;
  int c;
  int format=OUTPUTTERSE;
  
  while ((c=getopt(argc,argv,"vsp:t:")) != -1)
  {
    switch (c)
    {
      case 'p':
        maxproc=(unsigned int)atoi(optarg);
        break;
      case 't':
        argtimeout=(unsigned int)atoi(optarg);
        break;
      case 'v':
        format=OUTPUTVERBOSE;
        break;
      case 's':
        summary=1;
        break;
    }
  }

#ifdef DEBUG
  printf("multiexec Maxproc: %d\tTimeout: %d\n",maxproc,argtimeout);
#endif

  /* Assign our signal handler as the signal handler */
  act.sa_handler=sig_chld;

  /* We only want to handle sigchild signals         */
  sigemptyset(&act.sa_mask);
  
  /* We only are interested in signals from children *
   * termination, not stopping.                      */
  act.sa_flags=SA_NOCLDSTOP;
  
  if (sigaction(SIGCHLD, &act,&oldact)<0)
  {
    fprintf(stderr,"sigaction failed\n");
    exit(1);
  }

  /* Set up our list to store our process states */
  newlist(&head,&tail); 
  newlist(&failedhead,&failedtail);
  while (!feof(stdin))
  {
    if (fgets(buffer,sizeof(buffer)-4,stdin))
    {
      removelinefeeds(buffer);
#ifdef DEBUG
      printf("main: list status\n");
      printlist(&head);
      printf("main: end list status\n");
#endif
#ifdef DEBUG
      printf("main: Creating child process node for command: %s\n",getcommand(buffer));
#endif
      child=createchildnode(argtimeout,getlabel(buffer,label),getcommand(buffer));
      appendprocess(child,&tail);
#ifdef DEBUG
      printf("main: Forking command: %s\n",getcommand(buffer));
#endif
      forkchild(child);
      while (listlen > maxproc)
      {
#ifdef DEBUG
        if (!exitproc) printf("Reached max number of process\n");
#endif
#ifdef USESCHED
        sched_yield();
#endif
        usleep(900000);
        if (exitproc) 
        {
          exitproc=0;
          printalloutput(&head,format,argtimeout);
        }
#ifdef USESCHED
        else sched_yield();
#endif
#ifdef DEBUG
        printf("main: (maxproc) list status\n");
        printlist(&head);
        printf("main: (maxproc) end list status\n");
#endif

      }
      if (exitproc) 
      {
        exitproc=0;
        printalloutput(&head,format,argtimeout);
      }
#ifdef USESCHED
      sched_yield();
#else
      usleep(90000);
#endif
    }
    //else break;
  }
#ifdef DEBUG
  printf("\n------No more input, exiting------\n\n");
#endif
  while (!emptylist(&head))
  {
    printalloutput(&head,format,argtimeout);
    sleep(1);
#ifdef DEBUG
    printf("Still waiting for child\n");
    printlist(&head);
#endif
  }
  if (summary && (!emptylist(&failedhead)))
  {
    printf("\nFailed on\n");
    child=failedhead.next;
    while (child != child->next)
    {	
      printf("\t%s\n",child->label);
      child=child->next;
    }
  }
#ifdef DEBUG
  printf("No more children\n");
  printf("Children spawned: %ld\n",childcount);
#endif
  exit(0);
}
