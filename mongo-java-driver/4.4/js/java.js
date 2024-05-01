jQuery(document).ready(function(){
  $('.distroPicker').bootstrapToggle();
  $('.distroPicker').change(function () {
    var dataset = $('.distroPicker').get(0).dataset;
    console.log(dataset.off.toLowerCase())
    if ($('.distroPicker').prop('checked')) {
      $('.' + dataset.off.toLowerCase()).addClass('hidden');
      $('.' + dataset.on.toLowerCase()).removeClass('hidden');
    } else {
      $('.' + dataset.on.toLowerCase()).addClass('hidden');
      $('.' + dataset.off.toLowerCase()).removeClass('hidden');
    }
  });
});
