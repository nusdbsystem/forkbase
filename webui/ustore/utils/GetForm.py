from django import forms

class GetForm(forms.Form):
	key = forms.CharField(required=True)
	bransion = forms.CharField(required=True)
	option = forms.CharField(required=True)

	def getKey(self):
		return key