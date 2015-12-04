#Variables
Folder='/home/cab22/scratch/C_lab/R17v2/r17/iteration_0/'

#Modules
import glob, os
import time
import subprocess


#Parse slurm_queue
def SlurmQueue():
    squeue=subprocess.check_output(['squeue']) #Call squeue
    squeue=squeue.split('\n') #Split per line
    squeue=[s.split() for s in squeue] #Split per column
    squeue=[dict(zip(squeue[0],s)) for s in squeue[1:-1]] #Create a Dict and clean
    return squeue

Still_running=True
while Still_running:
    print "Status"    
    Still_running=False
    #Get a list of all subfolders
    subfolders=next(os.walk(Folder))[1]
    #Call slurm
    jobid_list=[s['JOBID'] for s in SlurmQueue()]

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
            print 'MD in %s without log'%(folder) 
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
                print 'MD in %s still running'%(folder) 
                continue
        #If not slurm.jobid check if most recent output older than md.log    
        except IOError:
            #Check most recent log
            for f in glob.glob("*.log"):
                if os.path.getmtime(f) > last_time:
                    Running=True
                    break
            if Running:	
	        #print 'Last slurm file in %s created before last log:\nLast Log:%s\nLast Slurm%s'%(folder,time.ctime(os.path.getmtime(f)),time.ctime(last_time))               
                Still_running=True
                print 'MD in %s still running'%(folder)               
                continue
        
        #Check if the ran stop due to time
        Due2Time=False    
        handle=open(last_slurm)
        for line in handle:
            if "CANCELLED" in line and "DUE TO TIME LIMIT" in line:
                Due2Time=True
        handle.close()
        
        if not Due2Time:
            print 'MD in %s has ended'%(folder)
            continue
        
        #Restart if needed
        if not Running and Due2Time:
            #Run sbatch
            with open('slurm.jobid','w+') as jobid:        
                subprocess.call(['sbatch','rst.slurm'],stdout=jobid)
            print 'Restarting %s'%folder
            Still_running=True
    time.sleep(600)
print "No more processes active"         
        
        
    
