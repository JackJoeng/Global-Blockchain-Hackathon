(function(){
  var config = {
    templateUrl: '/app/components/general_ledger/generalLedger.component.html',
    controller: generalLedgerCtrl
  }

  function generalLedgerCtrl(generalLedgerService){
    var ctrl = this;
    ctrl.ledger = generalLedgerService.expEntries;
  };

  angular
    .module('blockchainApp')
    .component('generalLedger', config)
})();