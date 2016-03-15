import slurm
options=slurm.Options()
options.option('ntasks',5)


print options
print slurm.queue
jid=slurm.send('sleep 10',options=options)
print slurm.status(jid)
slurm.cancel(jid)
print slurm.status(jid)

