(function(){
  var config = {
    templateUrl: '/app/components/receiptBox/receiptBox.component.html',
    controller: receiptBoxCtrl
  }

  function receiptBoxCtrl(ngoSelectionBoxService){
    var ctrl = this;
    ctrl.organization = ngoSelectionBoxService.getSaveOrganization;
    ctrl.project = ngoSelectionBoxService.getSaveOrganizationProject;
  }
  angular
    .module('blockchainApp')
    .component('receiptBox', config)
})();