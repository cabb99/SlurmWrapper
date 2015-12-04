#!/opt/apps/software/MPI/intel/2015.2.164/OpenMPI/1.8.6/Python/2.7.9/bin/python
import sys, time
from daemon import Daemon
import glob, os
import subprocess

#Variables
Folder='/home/cab22/scratch/C_lab/R17v2/r17v3/iteration_0/'

class MyDaemon(Daemon):
    def SlurmQueue(self):
        squeue=subprocess.check_output(['squeue']) #Call squeue
        squeue=squeue.split('\n') #Split per line
        squeue=[s.split() for s in squeue] #Split per column
        squeue=[dict(zip(squeue[0],s)) for s in squeue[1:-1]] #Create a Dict and clean
        return squeue        
        
    def run(self):
        
        Log_file=Folder+'daemon.log'   
        Still_running=True
        while Still_running:
            daemon_log=open(Log_file,'w+')
            #Kill daemon when pidfile is erased
             
            try:
                pf = file(self.pidfile,'r')
                pid,hostname = pf.read().strip().split('\t')
                pid = int(pid)
                pf.close()
                #daemon_log.write(str(self.pidfile))
            except IOError:
                pid = None 
                            
            if not pid:
                daemon_log.write("Daemon killed remotely.\nDaemon stop at %s\n"%time.ctime(time.time()))                
                daemon_log.close()   
                break
            #print dir(subprocess)
            daemon_log.write("Status at %s\n"%time.ctime(time.time()))
            Still_running=False
            #Get a list of all subfolders
            subfolders=next(os.walk(Folder))[1]
                       
            #Call slurm queue
            jobid_list=[s['JOBID'] for s in self.SlurmQueue()]
            
            for folder in subfolders:
                #Enter folder    
                f=Folder+folder
                os.chdir(f)    
                
                #Find most recent Output from slurm
                last_time=0
                last_slurm=None
                for f in glob.glob("slurm*.out"):
                    last_slurm=f if os.path.getmtime(f) > last_time else last_slurm
                    last_time=os.path.getmtime(f)
                if last_slurm==None: #First run
                    Running=True
                    Still_running=True
                    daemon_log.write('MD in %s without log\n'%folder)
                    continue        

                #Check if running
                Running=False    
                try:
                    #Open slurm.jobid and check if in Slurm Queue        
                    with open('slurm.jobid') as jobid:
                        jid=jobid.readline().split()[-1]
                    if jid in jobid_list:
                        Running=True
                        Still_running=True
                        daemon_log.write('MD in %s still running\n'%folder) 
                        continue
                #If not slurm.jobid check if most recent output older than md.log    
                except IOError:
                    #Check most recent log
                    for f in glob.glob("*.log"):
                        if os.path.getmtime(f) > last_time:
                            Running=True
                            break
                    if Running:              
                        Still_running=True
                        daemon_log.write('MD in %s still running\n'%folder)               
                        continue
                
                #Check if the ran stop due to time
                Due2Time=False    
                handle=open(last_slurm)
                for line in handle:
                    if "CANCELLED" in line and "DUE TO TIME LIMIT" in line:
                        Due2Time=True
                handle.close()
                
                if not Due2Time:
                    daemon_log.write('MD in %s has ended\n'%folder)
                    continue
                
                #Restart if needed
                if not Running and Due2Time:
                    #Run sbatch
                    with open('slurm.jobid','w+') as jobid:        
                        subprocess.call(['sbatch','rst.slurm'],stdout=jobid)
                    daemon_log.write('Restarting %s\n'%folder)
                    Still_running=True
            daemon_log.close()            
            time.sleep(20*60)
        daemon_log=open(Log_file,'a+')        
        daemon_log.write("No more processes active, Daemon stop at %s\n"%time.ctime(time.time()))  
        daemon_log.close()                     
        

if __name__ == "__main__":
        daemon = MyDaemon('/home/cab22/Git/Daemon/AutoRestart.pid')

        if len(sys.argv) == 2:
                if 'start' == sys.argv[1]:
                        daemon.start()
                elif 'stop' == sys.argv[1]:
                        daemon.stop()
                elif 'restart' == sys.argv[1]:
                        daemon.restart()
                elif 'test' == sys.argv[1]:
                    daemon.pidfile=('/home/cab22/Git/Daemon/dummy.pid')                    
                    daemon.run()
                else:
                        print "Unknown command"
                        sys.exit(2)
                sys.exit(0)
        else:
                print "usage: %s start|stop|restart|test" % sys.argv[0]
                sys.exit(2)
