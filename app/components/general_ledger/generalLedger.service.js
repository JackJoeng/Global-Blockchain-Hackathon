(function(){
  angular
    .module('blockchainApp')
    .factory('generalLedgerService', generalLedgerService)

    function generalLedgerService(){
      var expEntries = [
        {expense: 'Lab coat',
          amount: '$40'}, 
        {expense: 'Vile',
        amount: '$30'}
      ];

      var service = {
        expEntries: expEntries
      };

      return service;
    };
})();