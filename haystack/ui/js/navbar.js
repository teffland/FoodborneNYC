angular.module('haystack-ui').controller('NavCollapseController', [function() {
  var nav = this;
  nav.isNavCollapsed = true;
  nav.isCollapsed = false;
  nav.isCollapsedHorizontal = false;
}]);
