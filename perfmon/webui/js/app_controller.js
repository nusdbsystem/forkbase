(function($){ 
	
	'use strict';
	Monitor.AppController = function(options){
		this.op=$.extend({},Monitor.AppController.defaultOptions,options)  
		this.init(); 
		this.container=this.op.container;
		Monitor.app=this;
	}
	Monitor.AppController.defaultOptions={
		container:$("#sidebar")
	}
	Monitor.AppController.prototype={
		init:function(){ 
			console.log("app controller init...");
		},
		render:function(){
			this.container.empty();
			var html="";
			html+="<h3>Current Status</h3>"
				+"<table>"
				+"<tr>"
				+"<th title='host'>host</th><th title='process'>proc</th>";

			var attrs = Monitor.store.attrs;
			for(var attrName in attrs){
				html+="<th title='"+attrName+"'>"+Monitor.store.attr2short[attrName]+"</th>";
			}
			html+="</tr>";

			var hostIndex=0;
			for(var i in Monitor.store.hosts){
				hostIndex++;
				var host = Monitor.store.hosts[i];
				var procIndex=0;
				for(var j in host.procs){
					procIndex++;
					var proc = host.procs[j];
					html+="<tr>"
						+"<td class='host' title='"+host.name+"'>"+host.name.split("-")[0]+"</td>"
						+"<td class='proc' title='"+proc.name+"'>"+proc.name.split(":")[0]+"</td>";
					for(var attrName in attrs){
						var attr =proc.attrs[attrName];
						var value= attr.getLatestValue();
						var clazz=Monitor.getColorClass(attrName,value)
						html+="<td class='attr "+clazz+"' title='"+attrName+"'>"+value+"</td>";
					}
					html+="</tr>";
				}
			}
			this.container.html(html);
			this.bind();			
		},
		bind:function(){
			var me = this;
			this.container.find(".attr").click(function(){
				var $tr = $(this).parent();
				var hostName = $tr.find(".host").attr("title");
				var procName = $tr.find(".proc").attr("title");
				var attrName = $(this).attr("title");
				me.createChart(hostName,procName,attrName);
			});
			return;
		},
		createChart:function(host,proc,attr){
			var attr = Monitor.store.hosts[host].procs[proc].attrs[attr];
			attr.showChart();
		},
		removeChart:function(chartController){
			
			Monitor.store.hosts[hostName].procs[proc][attr].push(controller);
			 
		},
		start:function(){
		
		 
		}
	}
	
	
})(jQuery);


