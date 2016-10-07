app = angular.module('haystack-ui');

app.directive('document', function() {
  return {
    templateUrl:'../views/content.html',
    restrict: 'E',
    controller: function($scope) {
      $scope.getTemplateUrl = function() {
        return $scope.doc.type + '.html';
      };
    }
  };
});
