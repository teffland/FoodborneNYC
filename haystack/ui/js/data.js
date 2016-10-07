angular.module('haystack-ui').controller('dataController', ['$http', function($http) {
  var controller = this;
  controller.sortStatement = ['-rating', '-reviews.length'];
  $http.get('../js/500_yelp_businesses.json')
    .then(function(response) {
      controller.businesses = response.data;

      // get a flat list of documents
      controller.documents = [];
      controller.businesses.forEach(function(business) {
        business.reviews.forEach(function(review) {
          review.type = 'yelp';
          controller.documents.push(review);
        });
      });
      console.log(controller.documents[0].type);
    });

  controller.toggleSort = function() {
    this.sortStatement[0] = this.sortStatement[0].startsWith('-') ? 'rating' : '-rating';
  };
}]);
