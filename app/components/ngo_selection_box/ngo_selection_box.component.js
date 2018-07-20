(function(){
  var config = {
    templateUrl: '/app/components/ngo_selection_box/ngo_selection_box.component.html',
    controller: ngoSelectionBoxCtrl
  }

  function ngoSelectionBoxCtrl(ngoSelectionBoxService){
    var ctrl = this;

    ctrl.setSaveOrg = ngoSelectionBoxService.setSaveOrganization;
    ctrl.setSaveProject = ngoSelectionBoxService.setSaveOrganizationProject;
    ctrl.organizationProject;
    ctrl.organization = ngoSelectionBoxService.getOrganizationList();
    ctrl.project = ngoSelectionBoxService.getProject;
  }

  angular
    .module("blockchainApp")
    .component('ngoSelectionBox', config)
})();