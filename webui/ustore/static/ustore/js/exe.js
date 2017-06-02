var contents = {};
$(document).ready(function() {

	function post_request(url, data, handle_data, args=null, show_field=null) {
		var jqXHR = $.ajax({
			url: url,
			type: "POST",
			dataType: 'json',
			data: data,
			success: function(response) {
				handle_data(args, response.result);
				if (show_field != null){
					show_field.show()
				}
			}
		});
	};

	$('[name="exe"]').click(function(){
		var data = new FormData($('[name="exe_form"]')[0]);
		var jsonData = {};
	    for (var entry of data.entries()) {
	        jsonData[entry[0]] = entry[1];
	    }

	    post_request(exe_url, jsonData, text_data, $('[name="result"]'), $('[name="result_div"]'));
		// var jqXHR = $.ajax({
		// 	url: exe_url,
		// 	type: "POST",
		// 	dataType: 'json',
		// 	data: jsonData,
		// 	success: function(response) {
		// 		$('[name="result"]').text(response.result);
		// 		$('[name="result_div"]').show();
		// 	}
		// });
		return false;
	});

	$('#key li div.hint a').click(function(event){
		var key = $(this).parent().parent().find('span').text()
		if (this.name == "branch"){
			$("#key li div.hint").find('a[name="branch"]').css('color', 'grey');
			this.style.color = "#e05a5c";
			$.ajax({
				url: list_url,
				data: {'key': key},
				dataType: 'json',
				type: 'POST',
				success: function(response) {
					uis = []
					$.each(response.result, function(i, item) {
						uis.push("<li><span>" + item[0] + '</span><span style="padding-left:2em">' + item[1] + '</span></li>');
					});
					$('#branch_list > ul > li').remove();
					$('#branch_list > ul').append(uis.join(''));
					$('#branch_list span > span[name="key_name"]').text(key);
					$('#branch_list').show();
				}
			});
		} else {
			$("#key li div.hint").find('a[name="version"]').css('color', 'grey');
			this.style.color = "#e05a5c";
			$.ajax({
				url: latest_url,
				data: {'key': key},
				dataType: 'json',
				type: 'POST',
				success: function(response) {
					uis = []
					$.each(response.result, function(i, item) {
						uis.push("<li><span>" + item + '</span></li>');
					});
					$('#version_list > ul > li').remove();
					$('#version_list > ul').append(uis.join(''));
					$('#version_list span > span[name="key_name"]').text(key);
					$('#version_list').show();
				}
			});
		}
		return false;
	});

	// toggle menu
	$("#branch_list").on("mouseover", "ul li", function(e) {
		contents.hover_key = $("span:first", this).text();
		var x = '0px';
		var y = (this.offsetTop + this.offsetHeight) + 'px';
		var content = $("#branch_list ul div.dropdown-content")[0].style;
		content.top = y;
		content.left = x;
		content.position = 'absolute';
	});

	$("#version_list").on("mouseover", "ul li", function(e) {
		contents.hover_key = $("span", this).text();
		var x = '0px';
		var y = (this.offsetTop + this.offsetHeight) + 'px';
		var content = $("#version_list ul div.dropdown-content")[0].style;
		content.top = y;
		content.left = x;
		content.position = 'absolute';
	});


	// fill in target for merge
	$("#branch_list").on("click", "ul li", function(e) {
		if($("#exe").find('form[name="merge"]').is(":visible")){
			var content = $("#exe").find('form[name="merge"]');
			content.find('[name="bransion2"]').val($("span:first", this).text());
			content.find('[name="opt2"]').text('target branch');
		}
	});


	$("#version_list").on("click", "ul li", function(e) {
		var content = $("#exe").find('form[name="merge"]');
		if(content.is(":visible")){
			if (content.find('[name="opt1"]').text().indexOf('branch') < 0){
				content.find('[name="opt1"]').text('referal branch');
				content.find('[name="bransion1"]').val('');
			}
			content.find('[name="bransion2"]').val($("span:first", this).text());
			content.find('[name="opt2"]').text('target version');
		}
	});

	function val_data(element, data){
		element.val(data);
	}

	function text_data(element, data) {
		element.text(data);
	}

	$("#branch_list ul div.dropdown-content").on('click', 'span', function(e){
		$("#exe > form").hide();
		$("#exe > form > div").find('textarea[name="result"]').parent().hide();
		var key = $("#branch_list span span[name='key_name'").text();
		var branch = contents.hover_key;
		if($(this).text() == 'put' || $(this).text() == 'get') {
			var content = $("#exe").find('form[name="get-or-put"]');
			content.show();
			content.find('[name="key"]').val(key);
			content.find('[name="bransion"]').val(branch);
			content.find('[name="opt"]').text('branch');

			post_request(get_url, {'key': key, 'opt': 'branch', 'bransion': branch}, val_data, content.find('[name="value"]'));
		} else if ($(this).text() == 'merge') {
			var content = $('#exe').find('form[name="merge"]');
			content.show();
			content.find('[name="key"]').val(key);
			content.find('[name="bransion1"]').val(branch);
			content.find('[name="opt1"]').text('referal branch');
		} else if ($(this).text() == 'branch') {
			var content = $("#exe").find('form[name="branch"]');
			content.show();
			content.find('[name="key"]').val(key);
			content.find('[name="bransion"]').val(branch);
			content.find('[name="opt"]').text('old branch');
		} else if ($(this).text() == 'rename') {
			var content = $("#exe").find('form[name="rename"]');
			content.show();
			content.find('[name="key"]').val(key);
			content.find('[name="bransion"]').val(branch);
		}
		return false;
	});


	$("#version_list ul div.dropdown-content").on('click', 'span', function(e){
		$("#exe > form").hide();
		$("#exe > form > div").find('textarea[name="result"]').parent().hide();
		var key = $("#branch_list span span[name='key_name'").text();
		var branch = contents.hover_key;
		if($(this).text() == 'put' || $(this).text() == 'get') {
			var content = $("#exe").find('form[name="get-or-put"]');
			content.show();
			content.find('[name="key"]').val(key);
			content.find('[name="bransion"]').val(branch);
			content.find('[name="opt"]').text('version');
			var res = post_request(get_url, {'key': key, 'opt': 'branch', 'bransion': branch}, val_data, content.find('[name="value"]'));
		} else if ($(this).text() == 'merge') {
			var content = $('#exe').find('form[name="merge"]');
			content.show();
			content.find('[name="key"]').val(key);
			content.find('[name="bransion1"]').val(branch);
			content.find('[name="opt1"]').text('referal version');
		} else if ($(this).text() == 'branch') {
			var content = $("#exe").find('form[name="branch"]');
			content.show();
			content.find('[name="key"]').val(key);
			content.find('[name="bransion"]').val(branch);
			content.find('[name="opt"]').text('version');
		}
		return false;
	});

	$("#put").on('click', function(e){
		var content = $(this).parent().parent()[0];
		var opt = $(content).find('[name="opt"]').text()
		data = {'key': content['key'].value, 
			'opt': opt, 
			'bransion': content['bransion'].value,
			'value': content['value'].value}
		post_request(put_url, data, text_data, $(content['result']), $(content['result']).parent());
		return false;
	});


	$("#merge").on('click', function(e){
		var content = $(this).parent().parent()[0];
		var opt1 = $(content).find('[name="opt1"]').text()
		opt1 = opt1.indexOf('branch') >= 0 ? 'branch' : 'version';
		var opt2 = $(content).find('[name="opt2"]').text()
		opt2 = opt2.indexOf('branch') >= 0 ? 'branch' : 'version';
		data = {'key': content['key'].value, 
			'opt1': opt2, 
			'bransion1': content['bransion2'].value,
			'opt2': opt1, 
			'bransion2': content['bransion1'].value,
			'value': content['value'].value}
		post_request(merge_url, data, text_data, $(content['result']), $(content['result']).parent());
		return false;
	});

	$("#branch").on('click', function(e){
		var content = $(this).parent().parent()[0];
		var opt = $(content).find('[name="opt"]').text()
		opt = opt == 'old branch' ? 'branch' : 'version';
		data = {'key': content['key'].value, 
			'opt': opt, 
			'bransion': content['bransion'].value,
			'branch': content['new_branch'].value}
		post_request(branch_url, data, text_data, $(content['result']), $(content['result']).parent());
		return false;
	});

	$("#rename").on('click', function(e){
		var content = $(this).parent().parent()[0];
		data = {'key': content['key'].value, 
			'old_brc': content['bransion'].value,
			'new_brc': content['new_branch'].value}
		post_request(rename_url, data, text_data, $(content['result']), $(content['result']).parent());
		return false;
	});
});
// localStorage.setItem("versions", JSON.stringify(response.versions));
// myInstance.latestVersions = response;

