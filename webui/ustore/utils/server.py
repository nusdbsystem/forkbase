import subprocess

class Server(object):
	def __init__(self, addr):
		self.addr = addr
		print 'server init: ' + self.addr

	def run_on_server(self, method, queries, is_post=True):
		if is_post:
			cmd = 'curl -d "%s" %s/%s' % ('&'.join([key+'='+queries[key] for key in queries]), self.addr, method)
		else:
			cmd = 'curl -G "%s"/"%s"' % (self.addr, method)		
		print cmd

		try:
			response = subprocess.check_output(cmd, shell=True)
		except subprocess.CalledProcessError:
			response = None
		return response if response is None or response == '' else response.rstrip('\r\n')
	