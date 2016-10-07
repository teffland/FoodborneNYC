angular.module('haystack-ui')
  .controller('PagingController', function() {
    this.pageSize=50;
    this.currentPage=0;
    this.offset = this.pageSize * this.currentPage;

    this.pageChanged = function() {
      this.offset = this.pageSize * this.currentPage;
    }
  });
