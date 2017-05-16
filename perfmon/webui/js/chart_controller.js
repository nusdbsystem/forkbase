(function($){ 
	
	'use strict';
	Monitor.ChartController = function(options){
		this.op=$.extend({},Monitor.ChartController.defaultOptions,options)  
		
		this.host=this.op.host;
		this.proc=this.op.proc;
		this.attr=this.op.attr;
		this.container=this.op.container;

		this.parent = this.op.parent;
		this.init(); 
	}
	Monitor.ChartController.defaultOptions={
		container:$("#main")
	}
	Monitor.ChartController.prototype={
		init:function(){ 
			console.log("Chart controller init...");
			this.render();
			 
		},
		render:function(){
			var me = this;
			  
			var source = $("#chart_template").html();
			var template = Handlebars.compile(source); 
			var model={};
			model.title=me.attr+" ("+me.host+","+me.proc+")";
			var html= template(model);
			var chartOptions=$.extend({},Monitor.ChartDefines["spline"]);
			chartOptions.chart.height=350;
			chartOptions.chart.width=590;
			chartOptions.yAxis.title={text:""};
			chartOptions.series=[{
						name: me.attr,
						data:[]
					}]
			me.dom=$(html).prependTo(me.container).find(".highchart");
			me.dom.highcharts(chartOptions);
			 
			this.bind();			
		},
		bind:function(){
			
			var me =this;
			me.dom.parent().find(".btn").on("click",function(){
				 me.dom.parent().remove();
				 me.parent.removeController(); 
			});
			
		},
		addPoint:function(point){ 
			var series = this.dom.highcharts().series[0]; 
			var isThift=false;
			if(series.points.length>20){
				isThift=true;
			}
			series.addPoint([point.time*1000, point.value], true, isThift); 
		}
	}
	
	
})(jQuery);


