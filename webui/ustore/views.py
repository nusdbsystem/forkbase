from django.shortcuts import render, redirect
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .utils.GetForm import GetForm
from .utils.server import Server

server = None

@csrf_exempt
def login(request):
	if request.method == 'GET':
		return render(request, 'ustore/login.html')
	elif request.method == 'POST':
		global server
		server = Server(request.POST.get('ip'))
		return redirect('../list')
		return render(request, 'ustore/list.html')
	else:
		raise Exception("unknown request method for get: " + request.method)


@csrf_exempt
def uget(request):
	if server is None:
		return render(request, 'ustore/login.html')

	if request.method == 'GET':
		return render(request, 'ustore/get.html')
	elif request.method == 'POST':
		# return JsonResponse({'result': '%s %s %s value' % (request.POST.get('key'), request.POST.get('opt'), request.POST.get('bransion'))})

		queries = {'key': request.POST.get('key'), 
			request.POST.get('opt'): request.POST.get('bransion')}
		res = server.run_on_server('get', queries)
		return JsonResponse({'result': res or ''})
	else:
		raise Exception("unknown request method for get: " + request.method)

@csrf_exempt
def uput(request):
	if server is None:
		return render(request, 'ustore/login.html')

	if request.method == 'GET':
		return render(request, 'ustore/put.html')

	elif request.method == 'POST':
		# return JsonResponse({'result': '%s %s %s %s' % 
		# 	(request.POST.get('key'), request.POST.get('opt'), request.POST.get('bransion'), request.POST.get('value'))})

		queries = {'key': request.POST.get('key'), 
			request.POST.get('opt'): request.POST.get('bransion'), 
			'value': request.POST.get('value')}
		res = server.run_on_server('put', queries)
		return JsonResponse({'result': res or ''})
	else:
		raise Exception("unknown request method for put: " + request.method)

@csrf_exempt
def ubranch(request):
	if server is None:
		return render(request, 'ustore/login.html')

	if request.method == 'GET':
		return render(request, 'ustore/branch.html')

	elif request.method == 'POST':
		# return JsonResponse({'result': '%s %s %s %s' % 
		# 	(request.POST.get('key'), request.POST.get('opt'), request.POST.get('bransion'), request.POST.get('new_branch'))})

		queries = {'key': request.POST.get('key'), 
			'new_branch': request.POST.get('branch')}
		if request.POST.get('opt') == 'branch':
			queries['old_branch'] = request.POST.get('bransion')
		else:
			queries['version'] = request.POST.get('bransion')
		res = server.run_on_server('branch', queries)
		return JsonResponse({'result': res or ''})
	else:
		raise Exception("unknown request method for branch: " + request.method)

@csrf_exempt
def umerge(request):
	if server is None:
		return render(request, 'ustore/login.html')

	if request.method == 'GET':
		return render(request, 'ustore/merge.html')

	elif request.method == 'POST':
		# print request.POST
		# return JsonResponse({'result': 'bibi'})

		queries = {'key': request.POST.get('key'), 'value': request.POST.get('value')}
		if request.POST.get('opt1') == 'branch':
			queries['tgt_branch'] = request.POST.get('bransion1')
			if request.POST.get('opt2') == 'branch':
				queries['ref_branch'] = request.POST.get('bransion2')
			else:
				queries['ref_version1'] = request.POST.get('bransion2')
		else:
			if request.POST.get('opt2') != 'version':
				return JsonResponse({'result': 'cannot merge branch to version'})
			queries['ref_version1'] = request.POST.get('bransion1')
			queries['ref_version2'] = request.POST.get('bransion2')

		res = server.run_on_server('merge', queries)
		return JsonResponse({'result': res or ''})
	else:
		raise Exception("unknown request method for merge: " + request.method)

@csrf_exempt
def urename(request):
	if server is None:
		return render(request, 'ustore/login.html')

	if request.method == 'POST':
		# return JsonResponse({'result': '%s %s %s' % 
		# 	(request.POST.get('key'), request.POST.get('old_brc'), request.POST.get('new_brc'))})
		
		queries = {'key': request.POST.get('key'), 
			'old_branch': request.POST.get('old_brc'), 
			'new_branch': request.POST.get('new_brc')}
		res = server.run_on_server('rename', queries)
		return JsonResponse({'result': res})
	else:
		raise Exception('invalid request method for latest: ' + request.method)

@csrf_exempt
def ulist(request):
	if server is None:
		return render(request, 'ustore/login.html')

	if request.method == 'GET':
		res = server.run_on_server('list', {}, False)
		print res
		return render(request, 'ustore/list.html', context={'keys': [] if res is None else res.split('\r\n')})
	elif request.method == 'POST':
		key = request.POST.get('key')
		queries = {'key': key}
		branches = server.run_on_server('list', queries)
		res = [[brc, server.run_on_server('head', {'key': key, 'branch': brc}) or ''] for brc in branches.split('\r\n')]
		print res
		return JsonResponse({'result': res})
	else:
		raise Exception("unknown request method for list: " + request.method)

@csrf_exempt
def ulatest(request):
	if server is None:
		return render(request, 'ustore/login.html')

	if request.method == 'POST':
		key = request.POST.get('key')
		queries = {'key': key}
		res = server.run_on_server('latest', queries)
		return JsonResponse({'result': res.split('\r\n')})
	else:
		raise Exception('invalid request method for latest: ' + request.method)

@csrf_exempt
def udelete(request):
	if server is None:
		return render(request, 'ustore/login.html')

	if request.method == 'POST':
		queries = {'key': request.POST.get('key'), 'branch': request.POST.get('branch')}
		res = server.run_on_server('delete', queries)
		return JsonResponse({'result': res.split('\r\n')})
	else:
		raise Exception('invalid request method for delete: ' + request.method)