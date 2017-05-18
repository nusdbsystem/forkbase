/*global Ember, DS, Monitor:true */
(function(){

	window.Monitor = {};

  Monitor.API="http://localhost:8888";

	//use jquery's event framework to build message subscribe system
	$("<div id='eventElement'></div>").appendTo($("body")).hide();

	//all the events available, avoiding hard code

	var subscribes={};

	Monitor.subscribe=function(eventName,caller,func){

	 	if(!subscribes[eventName]) {  //this event is not subscribe yet
			subscribes[eventName]={};
			$("#eventElement").on(eventName,function(e,data){

				for(var index in subscribes[eventName]){
					var subscribe=subscribes[eventName][index];
					try{
						subscribe.func.call(subscribe.caller,data);
					}catch(e){
						console.log(e);
					}
				}
				return;
			})
		}

		subscribes[eventName][caller.id]={caller:caller,func:func};

	}
	Monitor.unSubscribe=function(eventName,caller){
		if(!subscribes[eventName]){
			return;
		}
		delete subscribes[eventName][caller.id];
	}

	Monitor.send = function(eventName,data){
		$("#eventElement").trigger(eventName,[data]);

	}

	Monitor.init=function(){
	 	Monitor.store.load(function(){
			var app=new Monitor.AppController();
			setInterval(function(){
				Monitor.store.load(function(){
					app.render();
				});
			},1000);

	 	});
	}
	Array.prototype.findBy=function(name,value){
		for(var index in this){
			if(this[index][name]==value){
				return this[index];
			}

		}
		return ;
	}
	Array.prototype.filterBy=function(name,value){
		var result=[];
		for(var index in this){
			if(this[index][name]==value){
				result.push(this[index]);
			}

		}
		return result;
	}
	Array.prototype.removeObj=function(obj){
		var index = this.indexOf(obj);
		if(index>-1) this.splice(index,1);
		return;
	}
	Array.prototype.removeBy=function(name,value){
		for(var index in this){
			if(this[index][name]==value){
				this.splice(index,1);
				return;
			}

		}
	}

	//serialize form to object
	$.fn.serializeObject = function()
	{
	    var o = {};
	    var a = this.serializeArray();
	    $.each(a, function() {
	        if (o[this.name] !== undefined) {
	            if (!o[this.name].push) {
	                o[this.name] = [o[this.name]];
	            }
	            o[this.name].push(this.value || '');
	        } else {
	            o[this.name] = this.value || '';
	        }
	    });
	    return o;
	};

	Monitor.Controller={};
	Monitor.Controller.idSequence=1;
	Monitor.Controller.nextId=function(){
		return Monitor.Controller.idSequence++;
	}

	Monitor.Colors=["#1f77b4", "#aec7e8", "#ff7f0e", "#ffbb78", "#2ca02c", "#98df8a", "#d62728", "#ff9896", "#9467bd", "#c5b0d5", "#8c564b", "#c49c94", "#e377c2", "#f7b6d2", "#7f7f7f", "#c7c7c7", "#bcbd22", "#dbdb8d", "#17becf", "#9edae5"];


	Monitor.Color=function(id){
		return Monitor.Colors[id%Monitor.Colors.length];
	}

	Monitor.getColorClass=function(attrName,value){
		if(attrName=="cpu"){
			if(value>=0.5){
				return "warning";
			}else{
				return "info";
			}
		}
		return "info";
	}

})();


