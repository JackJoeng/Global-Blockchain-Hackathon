(function(){
  angular
    .module("blockchainApp")
    .config(function($routeProvider){
      $routeProvider
        .when('/', {
          template: '<homepage />'
        })
        .when('/payment', {
          template: '<payment_page />'
        })
        .when('/receipt', {
          template: '<receipt_page />'
        })
    })
})();