(function(){
  angular
    .module('blockchainApp')
    .factory('ngoSelectionBoxService', ngoSelectionBoxService)

    function ngoSelectionBoxService(){
      var saveOrganization = {};
      var saveOrganizationProject = {};

      var ngo = [
        {organization: 'Amnesty Internationial',
        project: "Campaigns"}, 
        {organization: 'Doctors Without Borders',
        project: 'Research'}, 
        {organization: 'ISO',
        project: 'Research'}
      ];

      var service = {
        setSaveOrganization: setSaveOrganization,
        setSaveOrganizationProject: setSaveOrganizationProject,
        getSaveOrganization: getSaveOrganization,
        getSaveOrganizationProject: getSaveOrganizationProject,
        getOrganizationList: getOrganizationList,
        getProject: getProject,
      };

      return service;

      ////

      function setSaveOrganization(name){
        return saveOrganization = name;
      };

      function getSaveOrganization(){
        return saveOrganization;
      };
      
      function setSaveOrganizationProject(name){
        return saveOrganizationProject = name;
      };

      function getSaveOrganizationProject(){
        return saveOrganizationProject;
      };

      function getOrganizationList(){
        var i = 0;
        var array = [];
        for(; i < ngo.length; i++){
          array.push(ngo[i].organization);
        }

        return array;
      }

      function getProject(name){
        var i = 0;
        var array = [];
        for(; i < ngo.length; i++){
          if(ngo[i].organization === name){
            array.push(ngo[i].project);
          }
        }
        return array;
      }

    };
})();