(function(){
  var config = {
    templateUrl: '/app/components/payment_box/payment_box.component.html',
    controller: paymentBoxCtrl
  }

  function paymentBoxCtrl(){
    var ctrl = this;
    ctrl.cardNumber;
    ctrl.fullName;
    ctrl.month;
    ctrl.year;
    ctrl.cvv;
  }

  angular
    .module('blockchainApp')
    .component('paymentBox', config)
})();