import slurm,time
opt=slurm.Options()
opt.option('job-name','Test1')
opt.option('ntasks',1)



print options
print slurm.queue()
jid=slurm.send('echo Starting\nsleep 10\necho Processing\nsleep 10\necho end\n',options=options)
print slurm.status(jid)
slurm.cancel(jid)
print slurm.status(jid)
jid=slurm.send('echo Starting\nsleep 10\necho Processing\nsleep 10\necho end\n',options=options)
while slurm.status(jid)<>'
    time.sleep(2)
