$(function(){
	$('button').click(function(){
		var input_addr = $('#input_addr').val();
		$.ajax({
			url: '/explore',
			data: $('form').serialize(),
			type: 'GET',
			success: function(response){
				console.log(response);
			},
			error: function(error){
				console.log(error);
			}
		});
	});
});