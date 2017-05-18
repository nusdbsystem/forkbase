(function(){

	Monitor.store={
		hosts:{},  
		attrs:{},
		attr2short:{
			"cpu":"cpu",
			"vmem":"mem",
			"io_read":"ioR",
			"io_write":"ioW",
			"net_recv":"netR",
			"net_send":"netS",
			"rss":"rss"
		},
		load:function(callback){ //load all data
			$.ajax({
				url:Monitor.API,
				contentType:"text/json",
				success:function(data){ 
					data=JSON.parse(data);
					$.map(data.snapshots,parseSnapshot);  
					if(callback){
						callback();
					}
				},
				error:function(e){
					console.log(e); 
				}
			});
			
			function parseSnapshot(snapshot){
				var host=Monitor.store.hosts[snapshot.hname];
				if(!host){
					host=Monitor.store.hosts[snapshot.hname]={name:snapshot.hname,times:[],procs:{}};
				} 
				if(host.times.indexOf(snapshot.time)>-1){ //this snapshot is exist
					return; 
				}
				host.times.push(snapshot.time);

				$.each(snapshot.procs,function(index,item){
					var proc=host.procs[item.pname];
					if(!proc){
						proc=host.procs[item.pname]={name:item.pname,attrs:{}};
					} 
					for(var attrName in item){
						if(attrName!="pname"){
							Monitor.store.attrs[attrName]=true;
							
							attr=proc.attrs[attrName];
							if(!attr){
								attr=proc.attrs[attrName]=new Monitor.Attr(host.name,proc.name,attrName);
							}
							attr.addValue(snapshot.time,item[attrName]);
						}
						
					} 
				});
			 
			}
		}
	};
	Monitor.Attr=function(host,proc,name){
		this.host=host;
		this.proc=proc;
		this.name=name;
		this.scale=[];// a list of {time:"",value:""}
		this.controller=undefined;
	}
	Monitor.Attr.prototype={
		addValue:function(time,value){
			var point = {
				time:parseInt(time),
				value:parseFloat(value)
			}

			this.scale.push(point);
			if(this.controller){
				this.controller.addPoint(point)
			}
		},
		getLatestValue:function(){
			if(this.scale.length>0){
				return this.scale[this.scale.length-1].value;
			}
			return;
		},
		showChart:function(){
			this.controller = new Monitor.ChartController({
				host:this.host,
				proc:this.proc,
				attr:this.name,
				parent:this
			});
			if(this.scale.length>0){
				var len= this.scale.length;
				var begin =len>10?len-10:0;
				for(var i=begin;i<len;i++){
					
					this.controller.addPoint(this.scale[i]);
				}
			}
		}
	}
	Highcharts.setOptions({
		global: {
			useUTC: false
		}
	});
	Monitor.ChartDefines={
			"spline":{  
					chart: {
						type: 'spline',
						animation: Highcharts.svg, // don't animate in old IE
						marginRight: 10,
					},
					title: {
						text: ''
					},
					xAxis: {
						type: 'datetime',
						tickPixelInterval: 150
					},
					yAxis: {
						title: {
							text: 'Value'
						},
						plotLines: [{
							value: 0,
							width: 1,
							color: '#808080'
						}]
					},
					tooltip: {
						formatter: function () {
							return '<b>' + this.series.name + '</b><br/>' +
								Highcharts.dateFormat('%Y-%m-%d %H:%M:%S', this.x) + '<br/>' +
								Highcharts.numberFormat(this.y, 2);
						}
					},
					legend: {
						enabled: false
					},
					exporting: {
						enabled: false
					},
					series: [{
						name: 'Random data',
						data:[]
					}]
				} 
			}  
})();