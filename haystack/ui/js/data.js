var dataController = function($http) {
  var controller = this;
  controller.sortStatement = ['-rating', '-reviews.length'];
  $http.get('../js/500_yelp_businesses.json')
    .then(function(response) {
      controller.businesses = response.data;
    });

  controller.toggleSort = function() {
    this.sortStatement[0] = this.sortStatement[0].startsWith('-') ? 'rating' : '-rating';
  };
};

angular.module('haystackUI').controller('dataController', dataController);
