import { RoutingModule } from './routing.module';

describe('RoutingModule', () => {
  let routingModule: RoutingModule;

  beforeEach(() => {
    routingModule = new RoutingModule();
  });

  it('should create an instance', () => {
    expect(routingModule).toBeTruthy();
  });
});
