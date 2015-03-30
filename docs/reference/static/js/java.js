jQuery(document).ready(function(){
  $('.distroPicker').bootstrapToggle();
  $('.distroPicker').change(function () {
    if ($('.distroPicker').prop('checked')) {
      $('.gradle').addClass('hidden');
      $('.maven').removeClass('hidden');
    } else {
      $('.maven').addClass('hidden');
      $('.gradle').removeClass('hidden');
    }
  });
});
